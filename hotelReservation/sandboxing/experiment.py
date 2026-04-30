from __future__ import annotations

import argparse
import csv
import json
import os
import socket
import subprocess
import time
from dataclasses import replace
from datetime import datetime, timezone
from itertools import cycle, islice
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import yaml
import requests

from .capture_utils import add_empty_rate_fixtures, extract_request_corpus, load_ndjson, normalize_capture
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
        run_command(
            ["kubectl", "apply", "--server-side", "--force-conflicts", "-f", "-"],
            input=manifest_text,
            capture_output=True,
        )


def recreate_namespace(namespace: str) -> None:
    # Start from a clean sandbox so stale Consul registrations from prior runs
    # do not survive and route search traffic to outdated dummy pods.
    run_command(
        [
            "kubectl",
            "delete",
            "namespace",
            namespace,
            "--ignore-not-found=true",
            "--wait=true",
            "--timeout=180s",
        ],
        capture_output=True,
    )


def ensure_metrics_server() -> None:
    subprocess.run(
        ["minikube", "addons", "enable", "metrics-server"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _get_service_pod(service: str, namespace: str | None = None) -> str:
    return _get_service_pods(
        service,
        namespace=namespace,
        label_selector=f"io.kompose.service={service}",
    )[0]


def _get_service_pods(
    service: str,
    namespace: str | None = None,
    label_selector: str | None = None,
) -> List[str]:
    selector = label_selector or f"io.kompose.service={service}"
    args = ["kubectl", "get", "pods"]
    if namespace:
        args.extend(["-n", namespace])
    args.extend(["-l", selector, "-o", "json"])
    raw = run_command(args, capture_output=True).stdout
    payload = json.loads(raw)
    items = payload.get("items", [])
    ready_pods = []
    for pod in items:
        metadata = pod.get("metadata", {})
        if metadata.get("deletionTimestamp"):
            continue
        status = pod.get("status", {})
        if status.get("phase") != "Running":
            continue
        container_statuses = status.get("containerStatuses", [])
        if container_statuses and not all(item.get("ready") for item in container_statuses):
            continue
        name = metadata.get("name")
        if name:
            ready_pods.append(name)

    if ready_pods:
        return ready_pods
    if items:
        return [item["metadata"]["name"] for item in items if item.get("metadata", {}).get("name")]
    raise RuntimeError(f"No pods found for service {service}")


def _read_capture_file(pod: str, capture_path: str) -> str:
    return run_command(
        ["kubectl", "exec", pod, "--", "cat", capture_path],
        capture_output=True,
    ).stdout


def _reset_capture_file(pod: str, capture_path: str) -> None:
    run_command(
        ["kubectl", "exec", pod, "--", "sh", "-c", f": > {capture_path}"],
        capture_output=True,
    )


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


def _load_search_capture_requests() -> List[Dict[str, str]]:
    hotels = json.loads((REPO_ROOT / "hotelReservation" / "data" / "hotels.json").read_text())
    date_windows = [
        ("2015-04-09", "2015-04-10"),
        ("2015-04-09", "2015-04-17"),
        ("2015-04-10", "2015-04-11"),
        ("2015-04-17", "2015-04-18"),
        ("2015-04-17", "2015-04-24"),
        ("2015-04-20", "2015-04-24"),
    ]
    offsets = [
        (0.0, 0.0),
        (0.004, -0.004),
        (-0.006, 0.005),
    ]

    requests_to_capture: List[Dict[str, str]] = []
    seen = set()
    for hotel in hotels:
        address = hotel.get("address", {})
        lat = address.get("lat")
        lon = address.get("lon")
        if lat is None or lon is None:
            continue
        for in_date, out_date in date_windows:
            for lat_offset, lon_offset in offsets:
                request = {
                    "inDate": in_date,
                    "outDate": out_date,
                    "lat": f"{lat + lat_offset:.4f}",
                    "lon": f"{lon + lon_offset:.4f}",
                }
                key = tuple(request.items())
                if key in seen:
                    continue
                seen.add(key)
                requests_to_capture.append(request)

    if not requests_to_capture:
        raise RuntimeError("No capture requests could be generated from hotelReservation/data/hotels.json")
    return requests_to_capture


def _iter_capture_requests(requests_to_capture: Iterable[Dict[str, str]], warmup_requests: int) -> Iterable[Dict[str, str]]:
    request_list = list(requests_to_capture)
    total_requests = max(len(request_list), warmup_requests)
    if total_requests <= 0:
        return request_list
    return list(islice(cycle(request_list), total_requests))


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


def _open_pod_port_forwards(
    namespace: str,
    pod_names: Sequence[str],
    remote_port: int,
) -> tuple[List[str], List[subprocess.Popen]]:
    targets: List[str] = []
    processes: List[subprocess.Popen] = []
    try:
        for pod_name in pod_names:
            local_port = _reserve_local_port()
            process = subprocess.Popen(
                [
                    "kubectl",
                    "port-forward",
                    "-n",
                    namespace,
                    f"pod/{pod_name}",
                    f"{local_port}:{remote_port}",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            _wait_for_port_forward(process, local_port)
            processes.append(process)
            targets.append(f"127.0.0.1:{local_port}")
        return targets, processes
    except Exception:
        for process in processes:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        raise


def _wait_for_consul_service(namespace: str, service_name: str, timeout_seconds: int = 30) -> None:
    local_port = _reserve_local_port()
    port_forward = subprocess.Popen(
        [
            "kubectl",
            "port-forward",
            "-n",
            namespace,
            "svc/consul",
            f"{local_port}:8500",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_port_forward(port_forward, local_port)
        deadline = time.time() + timeout_seconds
        last_error = ""
        while time.time() < deadline:
            try:
                response = requests.get(
                    f"http://127.0.0.1:{local_port}/v1/catalog/service/{service_name}",
                    timeout=3,
                )
                response.raise_for_status()
                payload = response.json()
                if payload:
                    return
            except requests.RequestException as exc:
                last_error = str(exc)
            time.sleep(1)
    finally:
        port_forward.terminate()
        port_forward.wait(timeout=10)

    details = (
        f"Consul did not report any instances for service {service_name} "
        f"in namespace {namespace} after {timeout_seconds}s."
    )
    if last_error:
        details = f"{details} Last Consul error: {last_error}"
    raise RuntimeError(details)


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
        pod = _get_service_pod(service)
        _reset_capture_file(pod, f"/tmp/{service}.ndjson")

    frontend = config.capture.frontend_base_url.rstrip("/")
    capture_requests = _iter_capture_requests(_load_search_capture_requests(), config.capture.warmup_requests)
    for params in capture_requests:
        response = requests.get(
            f"{frontend}/hotels",
            params=params,
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
    geo = normalize_capture(load_ndjson(captured["geo"]))
    corpus = extract_request_corpus(load_ndjson(captured["search"]))
    rate = normalize_capture(load_ndjson(captured["rate"]))
    rate = add_empty_rate_fixtures(rate, geo, corpus)

    fixture_path = config.output_dir / "fixtures" / "geo.json"
    write_json(fixture_path, geo)
    artifacts["geo_fixture"] = fixture_path

    fixture_path = config.output_dir / "fixtures" / "rate.json"
    write_json(fixture_path, rate)
    artifacts["rate_fixture"] = fixture_path

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
    recreate_namespace(config.namespace)
    apply_manifest(manifest_path)
    for service in ("geo", "rate", "search"):
        run_command(
            [
                "kubectl",
                "rollout",
                "status",
                "-n",
                config.namespace,
                f"deployment/{service}",
                "--timeout=180s",
            ]
        )
    for service_name in ("srv-geo", "srv-rate", "srv-search"):
        _wait_for_consul_service(config.namespace, service_name)
    return manifest_path


def measure_msc(config: ExperimentConfig, artifacts: Dict[str, Path]) -> Dict[str, object]:
    corpus = json.loads(artifacts["search_requests"].read_text())["requests"]
    pod_names = _get_service_pods("search", namespace=config.namespace, label_selector="app=search")
    targets, port_forwards = _open_pod_port_forwards(config.namespace, pod_names, 8082)
    try:
        steps = []
        best_rps = 0
        violation_reason = ""
        for rps in range(config.load.start_rps, config.load.max_rps + 1, config.load.step_rps):
            result = run_search_step(
                repo_root=REPO_ROOT,
                targets=targets,
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
                "failures": result.failures,
                "count": result.count,
                "observed_cpu_millicores": observed_cpu,
            }
            if result.error_summary:
                row["error_summary"] = result.error_summary
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
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        write_json(config.output_dir / "results" / f"{config.service}-run-{timestamp}.json", payload)
        return payload
    finally:
        for port_forward in port_forwards:
            port_forward.terminate()
        for port_forward in port_forwards:
            try:
                port_forward.wait(timeout=10)
            except subprocess.TimeoutExpired:
                port_forward.kill()
                port_forward.wait(timeout=10)


def append_run_to_dataset(config: ExperimentConfig, run_result: Dict[str, object]) -> Path:
    dataset_path = config.output_dir / "model" / "dataset.csv"
    dataset_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["service", "cpu_millicores", "memory_mb", "replicas", "observed_cpu_millicores", "msc_rps"]
    row = {
        "service": run_result["service"],
        "cpu_millicores": run_result["cpu_millicores"],
        "memory_mb": run_result["memory_mb"],
        "replicas": run_result["replicas"],
        "observed_cpu_millicores": run_result["observed_cpu_millicores"],
        "msc_rps": run_result["msc_rps"],
    }

    write_header = not dataset_path.exists() or dataset_path.stat().st_size == 0
    with dataset_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    return dataset_path


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


def parse_int_list(raw: str) -> List[int]:
    values = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        values.append(int(token))
    if not values:
        raise argparse.ArgumentTypeError("expected at least one integer value")
    return values


def already_collected(dataset_path: Path, config: ExperimentConfig) -> bool:
    if not dataset_path.exists() or dataset_path.stat().st_size == 0:
        return False

    with dataset_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            try:
                if (
                    row["service"] == config.service
                    and int(row["cpu_millicores"]) == config.cpu_millicores
                    and int(row["replicas"]) == config.replicas
                ):
                    return True
            except (KeyError, ValueError):
                continue
    return False


def run_single_experiment(
    config: ExperimentConfig,
    *,
    skip_apply: bool,
    artifacts: Dict[str, Path],
) -> Dict[str, object]:
    if not skip_apply:
        render_and_apply_sandbox(config, artifacts)

    run_result = measure_msc(config, artifacts)
    append_run_to_dataset(config, run_result)
    # Model training is intentionally disabled for now.
    # fit_from_run(config, run_result)
    return run_result


def iter_sweep_configs(
    base_config: ExperimentConfig,
    cpu_configs: Sequence[int],
    replica_configs: Sequence[int],
) -> Iterable[ExperimentConfig]:
    for cpu_millicores in cpu_configs:
        for replicas in replica_configs:
            yield replace(
                base_config,
                cpu_millicores=cpu_millicores,
                replicas=replicas,
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("--skip-capture", action="store_true")
    parser.add_argument("--skip-apply", action="store_true")
    parser.add_argument(
        "--cpu-configs",
        type=parse_int_list,
        help="Comma-separated CPU millicore sweep, for example: 100,200,400,800",
    )
    parser.add_argument(
        "--replica-configs",
        type=parse_int_list,
        help="Comma-separated replica sweep, for example: 1,2,3",
    )
    args = parser.parse_args()

    base_config = ExperimentConfig.from_file(args.config)
    base_config.output_dir.mkdir(parents=True, exist_ok=True)
    ensure_metrics_server()

    if args.skip_capture:
        artifacts = {
            "geo_fixture": base_config.output_dir / "fixtures" / "geo.json",
            "rate_fixture": base_config.output_dir / "fixtures" / "rate.json",
            "search_requests": base_config.output_dir / "fixtures" / "search-requests.json",
        }
    else:
        artifacts = build_capture_artifacts(base_config)

    if args.cpu_configs or args.replica_configs:
        cpu_configs = args.cpu_configs or [base_config.cpu_millicores]
        replica_configs = args.replica_configs or [base_config.replicas]
        dataset_path = base_config.output_dir / "model" / "dataset.csv"
        total = len(cpu_configs) * len(replica_configs)

        print(
            f"Running search sweep for {total} configuration(s): "
            f"cpu={cpu_configs}, replicas={replica_configs}",
            flush=True,
        )
        completed = 0
        skipped = 0
        for index, config in enumerate(iter_sweep_configs(base_config, cpu_configs, replica_configs), start=1):
            if already_collected(dataset_path, config):
                skipped += 1
                print(
                    f"[{index}/{total}] service={config.service} "
                    f"cpu={config.cpu_millicores}m replicas={config.replicas} already collected, skipping",
                    flush=True,
                )
                continue

            print(
                f"[{index}/{total}] service={config.service} "
                f"cpu={config.cpu_millicores}m replicas={config.replicas}",
                flush=True,
            )
            run_result = run_single_experiment(
                config,
                skip_apply=args.skip_apply,
                artifacts=artifacts,
            )
            completed += 1
            print(
                f"  msc_rps={run_result['msc_rps']} "
                f"violation={run_result['violation_reason']}",
                flush=True,
            )

        print(
            f"Sweep complete. completed={completed} skipped={skipped} dataset={dataset_path}",
            flush=True,
        )
        return

    run_single_experiment(
        base_config,
        skip_apply=args.skip_apply,
        artifacts=artifacts,
    )


if __name__ == "__main__":
    main()
