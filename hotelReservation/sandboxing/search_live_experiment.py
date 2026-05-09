from __future__ import annotations

import argparse
import csv
import json
import subprocess
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from .config import ExperimentConfig, write_json
from .experiment import (
    _get_service_pods,
    _open_pod_port_forwards,
    already_collected,
    build_capture_artifacts,
    ensure_metrics_server,
    iter_sweep_configs,
    parse_int_list,
    run_command,
)
from .loadgen import run_search_step
from .metrics import sample_cpu


REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_NAMESPACE = "default"
BASELINE_LABEL = "io.kompose.service=search"
SEARCH_DEPLOYMENT = "search"
SEARCH_CONTAINER = "hotel-reserv-search"
SEARCH_PORT = 8082


def get_current_search_resources(namespace: str = BASELINE_NAMESPACE) -> Dict[str, object]:
    result = run_command(
        [
            "kubectl",
            "get",
            "deployment",
            SEARCH_DEPLOYMENT,
            "-n",
            namespace,
            "-o",
            "jsonpath={.spec.template.spec.containers[0].resources}|||{.spec.replicas}",
        ],
        capture_output=True,
    )
    resources_str, replicas_str = result.stdout.strip().split("|||")
    return {
        "resources": json.loads(resources_str),
        "replicas": int(replicas_str),
    }


def patch_search_deployment(
    cpu_millicores: int,
    memory_mb: int,
    replicas: int,
    namespace: str = BASELINE_NAMESPACE,
) -> None:
    cpu_str = f"{cpu_millicores}m"
    mem_str = f"{memory_mb}Mi"
    run_command(
        [
            "kubectl",
            "patch",
            "deployment",
            SEARCH_DEPLOYMENT,
            "-n",
            namespace,
            "--patch",
            json.dumps(
                {
                    "spec": {
                        "replicas": replicas,
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": SEARCH_CONTAINER,
                                        "resources": {
                                            "requests": {"cpu": cpu_str, "memory": mem_str},
                                            "limits": {"cpu": cpu_str, "memory": mem_str},
                                        },
                                    }
                                ]
                            }
                        },
                    }
                }
            ),
        ],
        capture_output=True,
    )
    run_command(
        [
            "kubectl",
            "rollout",
            "status",
            "-n",
            namespace,
            f"deployment/{SEARCH_DEPLOYMENT}",
            "--timeout=180s",
        ],
        capture_output=True,
    )
    time.sleep(5)


def restore_search_deployment(original: Dict[str, object], namespace: str = BASELINE_NAMESPACE) -> None:
    run_command(
        [
            "kubectl",
            "patch",
            "deployment",
            SEARCH_DEPLOYMENT,
            "-n",
            namespace,
            "--patch",
            json.dumps(
                {
                    "spec": {
                        "replicas": original["replicas"],
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": SEARCH_CONTAINER,
                                        "resources": original["resources"],
                                    }
                                ]
                            }
                        },
                    }
                }
            ),
        ],
        capture_output=True,
    )
    run_command(
        [
            "kubectl",
            "rollout",
            "status",
            "-n",
            namespace,
            f"deployment/{SEARCH_DEPLOYMENT}",
            "--timeout=180s",
        ],
        capture_output=True,
    )


def resolve_requests_path(config: ExperimentConfig, explicit_path: str | None) -> Path:
    candidates: List[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    candidates.append(config.output_dir / "fixtures" / "search-requests.json")
    candidates.append(REPO_ROOT / "sandboxing" / "output" / "search" / "fixtures" / "search-requests.json")

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Could not find search request corpus. "
        "Pass --requests-path or run capture first."
    )


def measure_live_search(config: ExperimentConfig, requests_path: Path) -> Dict[str, object]:
    corpus = json.loads(requests_path.read_text())["requests"]
    pod_names = _get_service_pods(
        "search",
        namespace=BASELINE_NAMESPACE,
        label_selector=BASELINE_LABEL,
    )
    targets, port_forwards = _open_pod_port_forwards(BASELINE_NAMESPACE, pod_names, SEARCH_PORT)
    try:
        steps = []
        best_rps = 0
        violation_reason = ""
        lo, hi = config.load.start_rps, config.load.max_rps

        while lo <= hi:
            rps = ((lo + hi) // 2 // config.load.step_rps) * config.load.step_rps
            rps = max(config.load.start_rps, rps)

            result = run_search_step(
                repo_root=REPO_ROOT,
                targets=targets,
                requests_corpus=corpus,
                rps=rps,
                duration_seconds=config.load.step_duration_seconds,
            )
            metrics = sample_cpu(BASELINE_NAMESPACE, BASELINE_LABEL)
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

            passed = (
                result.success_rate >= config.slo.success_rate_threshold
                and result.p90_latency_ms <= config.slo.p90_latency_ms
            )
            if passed:
                best_rps = rps
                lo = rps + config.load.step_rps
            else:
                violation_reason = (
                    "success_rate"
                    if result.success_rate < config.slo.success_rate_threshold
                    else "p90_latency"
                )
                hi = rps - config.load.step_rps

        payload = {
            "mode": "baseline",
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
        write_json(config.output_dir / "results" / f"{config.service}-live-run-{timestamp}.json", payload)
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
    fieldnames = [
        "service",
        "cpu_millicores",
        "memory_mb",
        "replicas",
        "observed_cpu_millicores",
        "msc_rps",
        "violation_reason",
    ]
    row = {
        "service": run_result["service"],
        "cpu_millicores": run_result["cpu_millicores"],
        "memory_mb": run_result["memory_mb"],
        "replicas": run_result["replicas"],
        "observed_cpu_millicores": run_result["observed_cpu_millicores"],
        "msc_rps": run_result["msc_rps"],
        "violation_reason": run_result["violation_reason"],
    }

    write_header = not dataset_path.exists() or dataset_path.stat().st_size == 0
    with dataset_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    return dataset_path


def run_single_live_experiment(config: ExperimentConfig, requests_path: Path) -> Dict[str, object]:
    original = get_current_search_resources()
    try:
        patch_search_deployment(
            cpu_millicores=config.cpu_millicores,
            memory_mb=config.memory_mb,
            replicas=config.replicas,
        )
        run_result = measure_live_search(config, requests_path)
    finally:
        restore_search_deployment(original)

    append_run_to_dataset(config, run_result)
    return run_result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure live search MSC against real geo and rate dependencies."
    )
    parser.add_argument("config")
    parser.add_argument("--skip-capture", action="store_true")
    parser.add_argument(
        "--requests-path",
        help="Path to an existing search-requests.json corpus. "
        "If omitted, the runner tries the config output dir and then sandboxing/output/search/fixtures/.",
    )
    parser.add_argument(
        "--cpu-configs",
        type=parse_int_list,
        help="Comma-separated CPU millicore sweep, for example: 100,200,500",
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
        requests_path = resolve_requests_path(base_config, args.requests_path)
    else:
        artifacts = build_capture_artifacts(base_config)
        requests_path = artifacts["search_requests"]

    if args.cpu_configs or args.replica_configs:
        cpu_configs = args.cpu_configs or [base_config.cpu_millicores]
        replica_configs = args.replica_configs or [base_config.replicas]
        dataset_path = base_config.output_dir / "model" / "dataset.csv"
        total = len(cpu_configs) * len(replica_configs)

        print(
            f"Running live search sweep for {total} configuration(s): "
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
            run_result = run_single_live_experiment(config, requests_path)
            completed += 1
            print(
                f"  msc_rps={run_result['msc_rps']} "
                f"violation={run_result['violation_reason']}",
                flush=True,
            )

        print(
            f"Live sweep complete. completed={completed} skipped={skipped} dataset={dataset_path}",
            flush=True,
        )
        return

    run_single_live_experiment(base_config, requests_path)


if __name__ == "__main__":
    main()
