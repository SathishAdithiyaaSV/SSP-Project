from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Dict, List

import yaml
import requests

from .capture_utils import extract_request_corpus, load_ndjson, normalize_capture
from .config import ExperimentConfig, write_json
from .loadgen import run_search_step
from .metrics import sample_cpu
from .model import fit_model
from .render import render_search_sandbox, write_manifest


REPO_ROOT = Path(__file__).resolve().parents[2]


def run_command(args: List[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(args, check=True, text=True, **kwargs)


def apply_manifest(path: Path) -> None:
    documents = list(yaml.safe_load_all(path.read_text()))
    for document in documents:
        if document is None:
            continue
        manifest_text = yaml.safe_dump(document)
        try:
            run_command(["kubectl", "create", "-f", "-"], input=manifest_text, capture_output=True)
        except subprocess.CalledProcessError as exc:
            output = (exc.stderr or "") + (exc.stdout or "")
            if "already exists" in output.lower():
                run_command(["kubectl", "replace", "-f", "-"], input=manifest_text, capture_output=True)
            else:
                raise


def ensure_metrics_server() -> None:
    subprocess.run(
        ["minikube", "addons", "enable", "metrics-server"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _get_service_pod(service: str) -> str:
    return (
        run_command(
            [
                "kubectl",
                "get",
                "pods",
                "-l",
                f"io.kompose.service={service}",
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ],
            capture_output=True,
        )
        .stdout.strip()
    )


def _read_capture_file(pod: str, capture_path: str) -> str:
    return run_command(
        ["kubectl", "exec", pod, "--", "cat", capture_path],
        capture_output=True,
    ).stdout


def _wait_for_capture_file(service: str, pod: str, capture_path: str, timeout_seconds: int = 30) -> str:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            raw = _read_capture_file(pod, capture_path)
            if raw.strip():
                return raw
        except subprocess.CalledProcessError as exc:
            last_error = (exc.stderr or exc.stdout or "").strip()
        time.sleep(1)

    details = (
        f"No capture data found at {capture_path} in pod {pod} for service {service} "
        f"after {timeout_seconds}s of warmup traffic."
    )
    if last_error:
        details = f"{details} Last kubectl error: {last_error}"
    raise RuntimeError(
        details
        + " The running baseline image likely does not include the sandbox capture interceptor yet. "
        + "Rebuild and reload `deathstarbench/hotel-reservation:latest`, redeploy the baseline services, then rerun the experiment."
    )


def _reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_port_forward(process: subprocess.Popen, local_port: int, timeout_seconds: int = 15) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr else ""
            raise RuntimeError(f"kubectl port-forward exited early: {stderr.strip()}")
        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=1):
                return
        except OSError:
            time.sleep(0.5)
    raise RuntimeError(f"timed out waiting for kubectl port-forward on localhost:{local_port}")


def capture_search_baseline(config: ExperimentConfig) -> Dict[str, Path]:
    if config.service != "search":
        raise NotImplementedError("v1 capture only supports search")

    for service in ("geo", "rate", "search"):
        run_command(
            [
                "kubectl",
                "set",
                "env",
                f"deployment/{service}",
                f"DSB_CAPTURE_PATH=/tmp/{service}.ndjson",
                f"DSB_CAPTURE_SERVICE={service}",
            ]
        )
        run_command(["kubectl", "rollout", "status", f"deployment/{service}", "--timeout=120s"])

    frontend = config.capture.frontend_base_url.rstrip("/")
    for _ in range(config.capture.warmup_requests):
        response = requests.get(
            f"{frontend}/hotels",
            params={
                "inDate": "2015-04-17",
                "outDate": "2015-04-18",
                "lat": "38.0235",
                "lon": "-122.095",
            },
            timeout=10,
        )
        response.raise_for_status()

    output_dir = config.output_dir / "capture"
    output_dir.mkdir(parents=True, exist_ok=True)
    saved = {}
    for service in ("geo", "rate", "search"):
        pod = _get_service_pod(service)
        raw = _wait_for_capture_file(service, pod, f"/tmp/{service}.ndjson")
        path = output_dir / f"{service}.ndjson"
        path.write_text(raw)
        saved[service] = path
    return saved


def build_capture_artifacts(config: ExperimentConfig) -> Dict[str, Path]:
    captured = capture_search_baseline(config)
    artifacts = {}
    for service in ("geo", "rate"):
        normalized = normalize_capture(load_ndjson(captured[service]))
        fixture_path = config.output_dir / "fixtures" / f"{service}.json"
        write_json(fixture_path, normalized)
        artifacts[f"{service}_fixture"] = fixture_path

    corpus = extract_request_corpus(load_ndjson(captured["search"]))
    corpus_path = config.output_dir / "fixtures" / "search-requests.json"
    write_json(corpus_path, corpus)
    artifacts["search_requests"] = corpus_path
    return artifacts


def render_and_apply_sandbox(config: ExperimentConfig, artifacts: Dict[str, Path]) -> Path:
    docs = render_search_sandbox(
        config=config,
        repo_root=REPO_ROOT,
        geo_fixture_path=artifacts["geo_fixture"],
        rate_fixture_path=artifacts["rate_fixture"],
    )
    manifest_path = config.output_dir / "sandbox" / "search-sandbox.yaml"
    write_manifest(manifest_path, docs)
    apply_manifest(manifest_path)
    run_command(["kubectl", "rollout", "status", "-n", config.namespace, "deployment/search", "--timeout=180s"])
    return manifest_path


def measure_msc(config: ExperimentConfig, artifacts: Dict[str, Path]) -> Dict[str, object]:
    corpus = json.loads(artifacts["search_requests"].read_text())["requests"]
    local_port = _reserve_local_port()
    target = f"127.0.0.1:{local_port}"
    port_forward = subprocess.Popen(
        [
            "kubectl",
            "port-forward",
            "-n",
            config.namespace,
            "deployment/search",
            f"{local_port}:8082",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_port_forward(port_forward, local_port)
        steps = []
        best_rps = 0
        violation_reason = ""
        for rps in range(config.load.start_rps, config.load.max_rps + 1, config.load.step_rps):
            result = run_search_step(
                repo_root=REPO_ROOT,
                target=target,
                requests_corpus=corpus,
                rps=rps,
                duration_seconds=config.load.step_duration_seconds,
            )
            metrics = sample_cpu(config.namespace, "app=search")
            observed_cpu = sum(item.cpu_millicores for item in metrics)
            row = {
                "rps": result.rps,
                "success_rate": result.success_rate,
                "p90_latency_ms": result.p90_latency_ms,
                "observed_cpu_millicores": observed_cpu,
            }
            steps.append(row)
            if result.success_rate >= config.slo.success_rate_threshold and result.p90_latency_ms <= config.slo.p90_latency_ms:
                best_rps = result.rps
            else:
                if result.success_rate < config.slo.success_rate_threshold:
                    violation_reason = "success_rate"
                else:
                    violation_reason = "p90_latency"
                break
        payload = {
            "service": config.service,
            "cpu_millicores": config.cpu_millicores,
            "memory_mb": config.memory_mb,
            "replicas": config.replicas,
            "steps": steps,
            "msc_rps": best_rps,
            "violation_reason": violation_reason or "max_rps_reached",
            "observed_cpu_millicores": max((step["observed_cpu_millicores"] for step in steps), default=0),
        }
        write_json(config.output_dir / "results" / "search-run.json", payload)
        return payload
    finally:
        port_forward.terminate()
        port_forward.wait(timeout=10)


def fit_from_run(config: ExperimentConfig, run_result: Dict[str, object]) -> Path:
    row = {
        "service": run_result["service"],
        "cpu_millicores": run_result["cpu_millicores"],
        "memory_mb": run_result["memory_mb"],
        "replicas": run_result["replicas"],
        "observed_cpu_millicores": run_result["observed_cpu_millicores"],
        "msc_rps": run_result["msc_rps"],
    }
    return fit_model([row], config.output_dir / "model")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("--skip-capture", action="store_true")
    parser.add_argument("--skip-apply", action="store_true")
    args = parser.parse_args()

    config = ExperimentConfig.from_file(args.config)
    config.output_dir.mkdir(parents=True, exist_ok=True)
    ensure_metrics_server()

    if args.skip_capture:
        artifacts = {
            "geo_fixture": config.output_dir / "fixtures" / "geo.json",
            "rate_fixture": config.output_dir / "fixtures" / "rate.json",
            "search_requests": config.output_dir / "fixtures" / "search-requests.json",
        }
    else:
        artifacts = build_capture_artifacts(config)

    if not args.skip_apply:
        render_and_apply_sandbox(config, artifacts)

    run_result = measure_msc(config, artifacts)
    fit_from_run(config, run_result)


if __name__ == "__main__":
    main()
