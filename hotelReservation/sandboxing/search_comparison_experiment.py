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
    measure_msc,
    parse_int_list,
    render_and_apply_sandbox,
    run_command,
)
from .loadgen import run_search_step
from .metrics import sample_cpu


SEARCH_DEPLOYMENT = "search"
SEARCH_CONTAINER = "hotel-reserv-search"
BASELINE_NAMESPACE = "default"
BASELINE_LABEL = "io.kompose.service=search"
SEARCH_PORT = 8082


def load_existing_sandbox_rows(dataset_path: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    with dataset_path.open(newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            rows.append(
                {
                    "service": raw["service"],
                    "cpu_millicores": int(raw["cpu_millicores"]),
                    "memory_mb": int(raw["memory_mb"]),
                    "replicas": int(raw["replicas"]),
                    "observed_cpu_millicores": int(raw["observed_cpu_millicores"]),
                    "msc_rps": int(raw["msc_rps"]),
                    "violation_reason": raw.get("violation_reason", "unknown"),
                }
            )
    return rows


def iter_existing_configs(
    base_config: ExperimentConfig,
    sandbox_rows: Sequence[Dict[str, object]],
) -> Iterable[tuple[ExperimentConfig, Dict[str, object]]]:
    for row in sandbox_rows:
        yield (
            replace(
                base_config,
                cpu_millicores=int(row["cpu_millicores"]),
                memory_mb=int(row["memory_mb"]),
                replicas=int(row["replicas"]),
            ),
            row,
        )


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


def measure_live_search(config: ExperimentConfig, artifacts: Dict[str, Path]) -> Dict[str, object]:
    corpus = json.loads(artifacts["search_requests"].read_text())["requests"]
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
                repo_root=Path(__file__).resolve().parents[2],
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
        write_json(config.output_dir / "results" / f"{config.service}-baseline-run-{timestamp}.json", payload)
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


def append_comparison_row(
    output_dir: Path,
    config: ExperimentConfig,
    baseline_result: Dict[str, object],
    sandbox_result: Dict[str, object],
) -> Path:
    comparison_path = output_dir / "comparison.csv"
    fieldnames = [
        "service",
        "cpu_millicores",
        "memory_mb",
        "replicas",
        "baseline_msc_rps",
        "baseline_violation_reason",
        "baseline_observed_cpu_millicores",
        "sandbox_msc_rps",
        "sandbox_violation_reason",
        "sandbox_observed_cpu_millicores",
        "msc_delta_rps",
        "msc_delta_percent_vs_baseline",
    ]
    write_header = not comparison_path.exists() or comparison_path.stat().st_size == 0
    baseline_msc = float(baseline_result["msc_rps"])
    sandbox_msc = float(sandbox_result["msc_rps"])
    delta = sandbox_msc - baseline_msc
    delta_pct = (delta / baseline_msc * 100.0) if baseline_msc > 0 else ""
    row = {
        "service": config.service,
        "cpu_millicores": config.cpu_millicores,
        "memory_mb": config.memory_mb,
        "replicas": config.replicas,
        "baseline_msc_rps": baseline_result["msc_rps"],
        "baseline_violation_reason": baseline_result["violation_reason"],
        "baseline_observed_cpu_millicores": baseline_result["observed_cpu_millicores"],
        "sandbox_msc_rps": sandbox_result["msc_rps"],
        "sandbox_violation_reason": sandbox_result["violation_reason"],
        "sandbox_observed_cpu_millicores": sandbox_result["observed_cpu_millicores"],
        "msc_delta_rps": delta,
        "msc_delta_percent_vs_baseline": delta_pct,
    }
    comparison_path.parent.mkdir(parents=True, exist_ok=True)
    with comparison_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    return comparison_path


def append_existing_sandbox_comparison_row(
    output_dir: Path,
    config: ExperimentConfig,
    baseline_result: Dict[str, object],
    sandbox_row: Dict[str, object],
) -> Path:
    comparison_path = output_dir / "comparison.csv"
    fieldnames = [
        "service",
        "cpu_millicores",
        "memory_mb",
        "replicas",
        "baseline_msc_rps",
        "baseline_violation_reason",
        "baseline_observed_cpu_millicores",
        "sandbox_msc_rps",
        "sandbox_violation_reason",
        "sandbox_observed_cpu_millicores",
        "msc_delta_rps",
        "msc_delta_percent_vs_baseline",
    ]
    write_header = not comparison_path.exists() or comparison_path.stat().st_size == 0
    baseline_msc = float(baseline_result["msc_rps"])
    sandbox_msc = float(sandbox_row["msc_rps"])
    delta = sandbox_msc - baseline_msc
    delta_pct = (delta / baseline_msc * 100.0) if baseline_msc > 0 else ""
    row = {
        "service": config.service,
        "cpu_millicores": config.cpu_millicores,
        "memory_mb": config.memory_mb,
        "replicas": config.replicas,
        "baseline_msc_rps": baseline_result["msc_rps"],
        "baseline_violation_reason": baseline_result["violation_reason"],
        "baseline_observed_cpu_millicores": baseline_result["observed_cpu_millicores"],
        "sandbox_msc_rps": sandbox_row["msc_rps"],
        "sandbox_violation_reason": sandbox_row.get("violation_reason", "unknown"),
        "sandbox_observed_cpu_millicores": sandbox_row["observed_cpu_millicores"],
        "msc_delta_rps": delta,
        "msc_delta_percent_vs_baseline": delta_pct,
    }
    comparison_path.parent.mkdir(parents=True, exist_ok=True)
    with comparison_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    return comparison_path


def run_comparison_for_config(
    config: ExperimentConfig,
    artifacts: Dict[str, Path],
    comparison_root: Path,
) -> Dict[str, Dict[str, object]]:
    baseline_config = replace(config, output_dir=comparison_root / "baseline")
    sandbox_config = replace(config, output_dir=comparison_root / "sandbox")

    original = get_current_search_resources()
    try:
        patch_search_deployment(
            cpu_millicores=config.cpu_millicores,
            memory_mb=config.memory_mb,
            replicas=config.replicas,
        )
        baseline_result = measure_live_search(baseline_config, artifacts)
    finally:
        restore_search_deployment(original)

    render_and_apply_sandbox(sandbox_config, artifacts)
    sandbox_result = measure_msc(sandbox_config, artifacts)
    append_comparison_row(comparison_root, config, baseline_result, sandbox_result)

    return {
        "baseline": baseline_result,
        "sandbox": sandbox_result,
    }


def run_baseline_only_comparison_for_config(
    config: ExperimentConfig,
    sandbox_row: Dict[str, object],
    comparison_root: Path,
    artifacts: Dict[str, Path],
) -> Dict[str, Dict[str, object]]:
    baseline_config = replace(config, output_dir=comparison_root / "baseline")
    original = get_current_search_resources()
    try:
        patch_search_deployment(
            cpu_millicores=config.cpu_millicores,
            memory_mb=config.memory_mb,
            replicas=config.replicas,
        )
        baseline_result = measure_live_search(baseline_config, artifacts)
    finally:
        restore_search_deployment(original)

    append_existing_sandbox_comparison_row(comparison_root, config, baseline_result, sandbox_row)
    return {
        "baseline": baseline_result,
        "sandbox": sandbox_row,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare live baseline search measurement against sandboxed search measurement."
    )
    parser.add_argument("config")
    parser.add_argument("--skip-capture", action="store_true")
    parser.add_argument(
        "--use-existing-sandbox-dataset",
        help="Read existing sandbox search results from a dataset.csv and run only live baseline measurements.",
    )
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
    ensure_metrics_server()

    shared_output_dir = base_config.output_dir
    shared_output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_capture:
        artifacts = {
            "geo_fixture": shared_output_dir / "fixtures" / "geo.json",
            "rate_fixture": shared_output_dir / "fixtures" / "rate.json",
            "search_requests": shared_output_dir / "fixtures" / "search-requests.json",
        }
    else:
        artifacts = build_capture_artifacts(base_config)

    comparison_root = shared_output_dir / "comparison"
    dataset_path = comparison_root / "comparison.csv"

    if args.use_existing_sandbox_dataset:
        sandbox_dataset = Path(args.use_existing_sandbox_dataset)
        sandbox_rows = load_existing_sandbox_rows(sandbox_dataset)
        total = len(sandbox_rows)
        print(
            f"Running baseline-only comparison against existing sandbox dataset for {total} configuration(s): "
            f"dataset={sandbox_dataset}",
            flush=True,
        )

        completed = 0
        skipped = 0
        for index, (config, sandbox_row) in enumerate(iter_existing_configs(base_config, sandbox_rows), start=1):
            if already_collected(dataset_path, config):
                skipped += 1
                print(
                    f"[{index}/{total}] service={config.service} "
                    f"cpu={config.cpu_millicores}m replicas={config.replicas} already compared, skipping",
                    flush=True,
                )
                continue

            print(
                f"[{index}/{total}] service={config.service} "
                f"cpu={config.cpu_millicores}m replicas={config.replicas}",
                flush=True,
            )
            results = run_baseline_only_comparison_for_config(config, sandbox_row, comparison_root, artifacts)
            completed += 1
            print(
                f"  baseline_msc={results['baseline']['msc_rps']} "
                f"sandbox_msc={results['sandbox']['msc_rps']} "
                f"delta={float(results['sandbox']['msc_rps']) - float(results['baseline']['msc_rps'])}",
                flush=True,
            )

        print(
            f"Baseline-only comparison complete. completed={completed} skipped={skipped} output={comparison_root}",
            flush=True,
        )
        return

    cpu_configs = args.cpu_configs or [base_config.cpu_millicores]
    replica_configs = args.replica_configs or [base_config.replicas]
    total = len(cpu_configs) * len(replica_configs)

    print(
        f"Running search baseline-vs-sandbox comparison for {total} configuration(s): "
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
                f"cpu={config.cpu_millicores}m replicas={config.replicas} already compared, skipping",
                flush=True,
            )
            continue

        print(
            f"[{index}/{total}] service={config.service} "
            f"cpu={config.cpu_millicores}m replicas={config.replicas}",
            flush=True,
        )
        results = run_comparison_for_config(config, artifacts, comparison_root)
        completed += 1
        print(
            f"  baseline_msc={results['baseline']['msc_rps']} "
            f"sandbox_msc={results['sandbox']['msc_rps']} "
            f"delta={float(results['sandbox']['msc_rps']) - float(results['baseline']['msc_rps'])}",
            flush=True,
        )

    print(
        f"Comparison complete. completed={completed} skipped={skipped} output={comparison_root}",
        flush=True,
    )


if __name__ == "__main__":
    main()
