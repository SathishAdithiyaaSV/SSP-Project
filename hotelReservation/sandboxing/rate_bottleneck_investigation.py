from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import subprocess
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import grpc
import matplotlib.pyplot as plt
import pandas as pd

from .leaf_experiment import (
    MEMCACHED_MAP,
    SERVICES,
    flush_memcached,
    get_service_pods,
    load_service_modules,
    open_pod_port_forwards,
    run_command,
    wait_for_ready_channel,
    write_json,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "hotelReservation" / "sandboxing" / "output" / "rate-investigation"
DATASET_OTHERS = REPO_ROOT / "graphAnalysis" / "dataset_others.csv"
RATE_LABEL = "io.kompose.service=rate"
RATE_DEPLOYMENT = "rate"
RATE_CONTAINER = "hotel-reserv-rate"
NODE_CONTAINER = "minikube"
PROFILE_SECONDS = 12
LOG_TAIL = 200


@dataclass
class StepMetrics:
    rps: int
    success_rate: float
    effective_rps: float
    p50_latency_ms: float
    p90_latency_ms: float
    p99_latency_ms: float
    count: int
    failures: int
    error_summary: List[Dict[str, Any]]


def quantile_ms(values: List[float], q: float) -> float:
    if not values:
        return float("inf")
    if len(values) == 1:
        return values[0]
    index = max(0, min(len(values) - 1, math.ceil(q * len(values)) - 1))
    return sorted(values)[index]


def summarize_error(exc: Exception) -> str:
    if isinstance(exc, grpc.RpcError):
        code = exc.code()
        code_name = code.name if code is not None else "UNKNOWN_CODE"
        details = exc.details() or ""
        return f"{code_name}: {details}".strip()
    return f"{type(exc).__name__}: {exc}".strip()


def get_current_resources() -> Dict[str, Any]:
    result = run_command(
        [
            "kubectl",
            "get",
            "deployment",
            RATE_DEPLOYMENT,
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


def patch_rate_deployment(cpu_millicores: int, replicas: int) -> None:
    cpu_str = f"{cpu_millicores}m"
    run_command(
        [
            "kubectl",
            "patch",
            "deployment",
            RATE_DEPLOYMENT,
            "--patch",
            json.dumps(
                {
                    "spec": {
                        "replicas": replicas,
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": RATE_CONTAINER,
                                        "resources": {
                                            "requests": {"cpu": cpu_str},
                                            "limits": {"cpu": cpu_str},
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
            f"deployment/{RATE_DEPLOYMENT}",
            "--timeout=180s",
        ],
        capture_output=True,
    )
    time.sleep(5)


def restore_rate_deployment(original: Dict[str, Any]) -> None:
    run_command(
        [
            "kubectl",
            "patch",
            "deployment",
            RATE_DEPLOYMENT,
            "--patch",
            json.dumps(
                {
                    "spec": {
                        "replicas": original["replicas"],
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": RATE_CONTAINER,
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
            f"deployment/{RATE_DEPLOYMENT}",
            "--timeout=180s",
        ],
        capture_output=True,
    )


def run_step_detailed(
    targets: Sequence[str],
    rps: int,
    duration_seconds: int,
    timeout_seconds: float,
) -> StepMetrics:
    spec = SERVICES["rate"]
    proto_module, grpc_module = load_service_modules(spec)
    request_cls = getattr(proto_module, spec.request_class)
    total = max(1, rps * duration_seconds)
    latencies: List[float] = []
    successes = 0
    errors: Counter[str] = Counter()

    channels = [grpc.insecure_channel(target) for target in targets]
    stubs = [getattr(grpc_module, spec.stub_class)(channel) for channel in channels]
    methods = [getattr(stub, spec.method_name) for stub in stubs]

    def invoke(payload: Dict[str, Any], method: Any) -> float:
        request = request_cls(**payload)
        start = time.perf_counter()
        method(request, timeout=timeout_seconds, wait_for_ready=True)
        return (time.perf_counter() - start) * 1000

    try:
        for channel in channels:
            wait_for_ready_channel(channel)
        for method in methods:
            try:
                invoke(spec.payloads[0], method)
            except Exception as exc:  # pragma: no cover - warmup errors are informative only
                errors[summarize_error(exc)] += 1

        with ThreadPoolExecutor(max_workers=min(200, max(1, rps))) as executor:
            futures = []
            for index in range(total):
                payload = spec.payloads[index % len(spec.payloads)]
                method = methods[index % len(methods)]
                futures.append(executor.submit(invoke, payload, method))
                if (index + 1) % max(1, rps) == 0:
                    time.sleep(1)

            for future in as_completed(futures):
                try:
                    latencies.append(future.result())
                    successes += 1
                except Exception as exc:
                    errors[summarize_error(exc)] += 1
    finally:
        for channel in channels:
            channel.close()

    success_rate = successes / total
    effective_rps = successes / max(1, duration_seconds)
    return StepMetrics(
        rps=rps,
        success_rate=success_rate,
        effective_rps=effective_rps,
        p50_latency_ms=quantile_ms(latencies, 0.50),
        p90_latency_ms=quantile_ms(latencies, 0.90),
        p99_latency_ms=quantile_ms(latencies, 0.99),
        count=total,
        failures=total - successes,
        error_summary=[
            {"error": error, "count": count}
            for error, count in errors.most_common()
        ],
    )


def discover_msc(
    replicas: int,
    duration_seconds: int,
    timeout_seconds: float,
    start_rps: int,
    step_rps: int,
    max_rps: int,
    success_threshold: float,
    p90_threshold_ms: int,
    flush_cache_enabled: bool,
) -> Dict[str, Any]:
    lo, hi = start_rps, max_rps
    best_rps = 0
    violation_reason = ""
    evaluated_steps: List[Dict[str, Any]] = []

    while lo <= hi:
        rps = ((lo + hi) // 2 // step_rps) * step_rps
        rps = max(start_rps, rps)
        if flush_cache_enabled:
            flush_memcached("rate")
        _, targets, port_forwards = wait_for_rate_targets(replicas)
        try:
            step = run_step_detailed(targets, rps, duration_seconds, timeout_seconds)
        finally:
            for process in port_forwards:
                process.terminate()
            for process in port_forwards:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
        step_payload = asdict(step)
        evaluated_steps.append(step_payload)

        passed = (
            step.success_rate >= success_threshold
            and step.p90_latency_ms <= p90_threshold_ms
        )
        if passed:
            best_rps = rps
            lo = rps + step_rps
        else:
            violation_reason = (
                "success_rate" if step.success_rate < success_threshold else "p90_latency"
            )
            hi = rps - step_rps

    return {
        "msc_rps": best_rps,
        "violation_reason": violation_reason or "max_rps_reached",
        "search_steps": evaluated_steps,
    }


def rounded_unique(values: Iterable[int], step_rps: int) -> List[int]:
    seen = set()
    ordered: List[int] = []
    for value in values:
        rounded = max(step_rps, int(round(value / step_rps) * step_rps))
        if rounded not in seen:
            seen.add(rounded)
            ordered.append(rounded)
    return ordered


def build_probe_loads(msc_rps: int, step_rps: int, max_rps: int) -> List[int]:
    if msc_rps <= 0:
        return [step_rps, step_rps * 2, step_rps * 3]
    return rounded_unique(
        [
            max(step_rps, int(msc_rps * 0.5)),
            max(step_rps, int(msc_rps * 0.8)),
            msc_rps,
            min(max_rps, msc_rps + step_rps),
        ],
        step_rps,
    )


def wait_for_rate_targets(expected_replicas: int, timeout_seconds: int = 120) -> tuple[List[str], List[str], List[subprocess.Popen]]:
    deadline = time.time() + timeout_seconds
    last_error: str | None = None
    while time.time() < deadline:
        port_forwards: List[subprocess.Popen] = []
        try:
            pod_names = get_service_pods("default", RATE_LABEL)
            if len(pod_names) != expected_replicas:
                last_error = f"expected {expected_replicas} replicas, found {len(pod_names)}"
                time.sleep(3)
                continue
            targets, port_forwards = open_pod_port_forwards("default", pod_names, SERVICES["rate"].port)
            channels = [grpc.insecure_channel(target) for target in targets]
            try:
                for channel in channels:
                    wait_for_ready_channel(
                        channel,
                        attempts=3,
                        timeout_seconds=5,
                        backoff_seconds=2,
                    )
            finally:
                for channel in channels:
                    channel.close()
            return pod_names, targets, port_forwards
        except Exception as exc:
            last_error = str(exc)
            for process in port_forwards:
                process.terminate()
            for process in port_forwards:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            time.sleep(3)
    raise RuntimeError(f"rate pods never became gRPC-ready: {last_error or 'unknown error'}")


def run_text_command(args: List[str]) -> str:
    return run_command(args, capture_output=True).stdout


def parse_cpu_stat(text: str) -> Dict[str, int]:
    metrics: Dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit():
            metrics[parts[0]] = int(parts[1])
    return metrics


def cpu_stat_delta(before: Dict[str, int], after: Dict[str, int]) -> Dict[str, int]:
    return {key: after.get(key, 0) - before.get(key, 0) for key in set(before) | set(after)}


def get_pod_metadata(pod: str) -> Dict[str, Any]:
    raw = run_text_command(["kubectl", "get", "pod", pod, "-o", "json"])
    return json.loads(raw)


def get_restart_count(pod: str) -> int:
    payload = get_pod_metadata(pod)
    statuses = payload.get("status", {}).get("containerStatuses", [])
    if not statuses:
        return 0
    return int(statuses[0].get("restartCount", 0))


def get_cpu_stat(pod: str) -> Dict[str, int]:
    raw = run_text_command(["kubectl", "exec", pod, "--", "cat", "/sys/fs/cgroup/cpu.stat"])
    return parse_cpu_stat(raw)


def get_mem_stat(pod: str) -> Dict[str, int]:
    values: Dict[str, int] = {}
    for name in ("memory.current", "memory.peak"):
        try:
            raw = run_text_command(["kubectl", "exec", pod, "--", "cat", f"/sys/fs/cgroup/{name}"]).strip()
            values[name] = int(raw)
        except Exception:
            continue
    return values


def pod_top_table() -> List[Dict[str, Any]]:
    raw = run_text_command(["kubectl", "top", "pod", "-l", RATE_LABEL, "--no-headers"])
    rows: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        cpu_text = parts[1]
        mem_text = parts[2]
        cpu_m = int(cpu_text[:-1]) if cpu_text.endswith("m") else int(float(cpu_text) * 1000)
        mem_mib = int(mem_text[:-2]) if mem_text.endswith("Mi") else mem_text
        rows.append(
            {
                "pod": parts[0],
                "cpu_millicores": cpu_m,
                "memory": mem_text,
                "memory_mib": mem_mib,
            }
        )
    return rows


def describe_pod(pod: str) -> str:
    return run_text_command(["kubectl", "describe", "pod", pod])


def pod_logs(pod: str) -> str:
    return run_text_command(["kubectl", "logs", "--tail", str(LOG_TAIL), pod])


def extract_log_errors(log_text: str) -> List[str]:
    matches = []
    pattern = re.compile(r"(error|panic|fail|fatal|throttl|oom)", re.IGNORECASE)
    for line in log_text.splitlines():
        if pattern.search(line):
            matches.append(line)
    return matches[-20:]


def ensure_node_tools() -> None:
    check = subprocess.run(
        ["docker", "exec", NODE_CONTAINER, "sh", "-lc", "which perf && which pidstat"],
        text=True,
        capture_output=True,
    )
    if check.returncode == 0:
        return

    run_command(
        [
            "docker",
            "exec",
            NODE_CONTAINER,
            "sh",
            "-lc",
            "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y linux-perf sysstat",
        ],
        capture_output=True,
    )


def get_container_id_for_pod(pod: str) -> str:
    return run_text_command(
        ["kubectl", "get", "pod", pod, "-o", "jsonpath={.status.containerStatuses[0].containerID}"]
    ).strip().split("://", 1)[-1]


def get_node_pid_for_container(container_id: str) -> int:
    raw = run_text_command(["docker", "exec", NODE_CONTAINER, "crictl", "inspect", container_id])
    payload = json.loads(raw)
    return int(payload["info"]["pid"])


def run_node_command(command: str, timeout: int | None = None) -> str:
    return run_command(
        ["docker", "exec", NODE_CONTAINER, "sh", "-lc", command],
        capture_output=True,
        timeout=timeout,
    ).stdout


def collect_perf_for_pid(pid: int, seconds: int) -> str:
    command = (
        "perf stat -x, "
        "-e cycles,instructions,context-switches,cache-misses,cpu-migrations "
        f"-p {pid} sleep {seconds}"
    )
    completed = subprocess.run(
        ["docker", "exec", NODE_CONTAINER, "sh", "-lc", command],
        text=True,
        capture_output=True,
        timeout=seconds + 20,
    )
    return (completed.stdout + "\n" + completed.stderr).strip()


def collect_pidstat_for_pid(pid: int, seconds: int) -> str:
    command = f"pidstat -wt -p {pid} 1 {seconds}"
    return run_node_command(command, timeout=seconds + 20)


def collect_schedstat_for_pid(pid: int) -> Dict[str, Any]:
    try:
        raw = run_node_command(f"cat /proc/{pid}/schedstat").strip()
    except Exception:
        return {}
    parts = raw.split()
    if len(parts) < 3:
        return {}
    return {
        "runtime_ns": int(parts[0]),
        "runqueue_wait_ns": int(parts[1]),
        "timeslices": int(parts[2]),
    }


def parse_perf_csv(raw: str) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}
    for line in raw.splitlines():
        if "," not in line:
            continue
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 3:
            continue
        value, _, event = parts[:3]
        normalized_event = event
        if "/" in event:
            normalized_event = event.split("/")[-2]
        if value in {"<not counted>", "<not supported>", ""}:
            metrics[normalized_event] = value
            continue
        try:
            numeric = float(value)
            parsed = int(numeric) if numeric.is_integer() else numeric
            if normalized_event in metrics and isinstance(metrics[normalized_event], (int, float)):
                metrics[normalized_event] += parsed
            elif normalized_event not in metrics or metrics[normalized_event] in {"<not counted>", "<not supported>"}:
                metrics[normalized_event] = parsed
        except ValueError:
            metrics[normalized_event] = value
    return metrics


def parse_pidstat(raw: str) -> Dict[str, Any]:
    summaries: Dict[str, Any] = {"samples": []}
    for line in raw.splitlines():
        if not re.match(r"^\d{2}:\d{2}:\d{2}", line):
            continue
        if "UID" in line or "Command" in line and "cswch/s" in line:
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        try:
            sample = {
                "time": parts[0],
                "tgid": parts[2],
                "tid": parts[3],
                "cswch_per_s": float(parts[4]),
                "nvcswch_per_s": float(parts[5]),
                "command": parts[6] if len(parts) > 6 else "",
            }
        except ValueError:
            continue
        summaries["samples"].append(sample)

    if summaries["samples"]:
        summaries["avg_cswch_per_s"] = statistics.mean(item["cswch_per_s"] for item in summaries["samples"])
        summaries["avg_nvcswch_per_s"] = statistics.mean(item["nvcswch_per_s"] for item in summaries["samples"])
        summaries["max_nvcswch_per_s"] = max(item["nvcswch_per_s"] for item in summaries["samples"])
    else:
        summaries["avg_cswch_per_s"] = 0.0
        summaries["avg_nvcswch_per_s"] = 0.0
        summaries["max_nvcswch_per_s"] = 0.0
    return summaries


def run_profile_window(
    pod_names: Sequence[str],
    targets: Sequence[str],
    rps: int,
    duration_seconds: int,
    timeout_seconds: float,
    flush_cache_enabled: bool,
) -> Dict[str, Any]:
    if flush_cache_enabled:
        flush_memcached("rate")

    pod_state_before = {
        pod: {
            "cpu_stat": get_cpu_stat(pod),
            "mem_stat": get_mem_stat(pod),
            "restart_count": get_restart_count(pod),
        }
        for pod in pod_names
    }
    node_pids = {
        pod: get_node_pid_for_container(get_container_id_for_pod(pod))
        for pod in pod_names
    }
    sched_before = {pod: collect_schedstat_for_pid(pid) for pod, pid in node_pids.items()}

    perf_results: Dict[str, Any] = {}
    pidstat_results: Dict[str, Any] = {}
    k8s_snapshot: Dict[str, Any] = {}

    def load_worker() -> None:
        k8s_snapshot["load_result"] = asdict(
            run_step_detailed(targets, rps, duration_seconds, timeout_seconds)
        )

    load_thread = threading.Thread(target=load_worker)
    load_thread.start()

    perf_threads = {}
    pidstat_threads = {}

    def perf_worker(pod: str, pid: int) -> None:
        perf_results[pod] = collect_perf_for_pid(pid, PROFILE_SECONDS)

    def pidstat_worker(pod: str, pid: int) -> None:
        pidstat_results[pod] = collect_pidstat_for_pid(pid, PROFILE_SECONDS)

    for pod, pid in node_pids.items():
        perf_threads[pod] = threading.Thread(target=perf_worker, args=(pod, pid))
        pidstat_threads[pod] = threading.Thread(target=pidstat_worker, args=(pod, pid))
        perf_threads[pod].start()
        pidstat_threads[pod].start()

    time.sleep(max(2, duration_seconds // 3))
    k8s_snapshot["kubectl_top"] = pod_top_table()

    load_thread.join()
    for thread in perf_threads.values():
        thread.join()
    for thread in pidstat_threads.values():
        thread.join()

    pod_state_after = {
        pod: {
            "cpu_stat": get_cpu_stat(pod),
            "mem_stat": get_mem_stat(pod),
            "restart_count": get_restart_count(pod),
            "describe": describe_pod(pod),
            "logs": pod_logs(pod),
        }
        for pod in pod_names
    }
    sched_after = {pod: collect_schedstat_for_pid(pid) for pod, pid in node_pids.items()}

    per_pod = {}
    for pod in pod_names:
        per_pod[pod] = {
            "node_pid": node_pids[pod],
            "restart_count_before": pod_state_before[pod]["restart_count"],
            "restart_count_after": pod_state_after[pod]["restart_count"],
            "cpu_stat_before": pod_state_before[pod]["cpu_stat"],
            "cpu_stat_after": pod_state_after[pod]["cpu_stat"],
            "cpu_stat_delta": cpu_stat_delta(
                pod_state_before[pod]["cpu_stat"],
                pod_state_after[pod]["cpu_stat"],
            ),
            "mem_stat_before": pod_state_before[pod]["mem_stat"],
            "mem_stat_after": pod_state_after[pod]["mem_stat"],
            "schedstat_before": sched_before[pod],
            "schedstat_after": sched_after[pod],
            "perf_raw": perf_results[pod],
            "perf_summary": parse_perf_csv(perf_results[pod]),
            "pidstat_raw": pidstat_results[pod],
            "pidstat_summary": parse_pidstat(pidstat_results[pod]),
            "describe": pod_state_after[pod]["describe"],
            "logs_tail": pod_state_after[pod]["logs"],
            "log_errors": extract_log_errors(pod_state_after[pod]["logs"]),
        }
        before_sched = sched_before[pod]
        after_sched = sched_after[pod]
        if before_sched and after_sched:
            wait_delta = after_sched["runqueue_wait_ns"] - before_sched["runqueue_wait_ns"]
            runtime_delta = after_sched["runtime_ns"] - before_sched["runtime_ns"]
            slice_delta = after_sched["timeslices"] - before_sched["timeslices"]
            per_pod[pod]["schedstat_delta"] = {
                "runtime_ns": runtime_delta,
                "runqueue_wait_ns": wait_delta,
                "timeslices": slice_delta,
                "runqueue_wait_ms": wait_delta / 1_000_000,
                "runqueue_wait_pct_of_runtime": (
                    (wait_delta / runtime_delta) if runtime_delta > 0 else None
                ),
            }

    return {
        "rps": rps,
        "profile_seconds": PROFILE_SECONDS,
        "k8s_snapshot": k8s_snapshot,
        "per_pod": per_pod,
    }


def existing_baseline_msc() -> Dict[int, float]:
    baselines: Dict[int, float] = {}
    if not DATASET_OTHERS.exists():
        return baselines
    df = pd.read_csv(DATASET_OTHERS)
    rate_df = df[(df["service"] == "rate") & (df["replicas"] == 1)]
    for _, row in rate_df.iterrows():
        baselines[int(row["cpu_millicores"])] = float(row["msc_rps"])
    return baselines


def diagnose_run(run: Dict[str, Any], cpu_millicores: int, replicas: int) -> str:
    top_rows = run["profile"]["k8s_snapshot"].get("kubectl_top", [])
    avg_cpu = statistics.mean(row["cpu_millicores"] for row in top_rows) if top_rows else 0
    cpu_limit_total = cpu_millicores * replicas
    cpu_util_pct = (avg_cpu / cpu_limit_total * 100) if cpu_limit_total else 0

    throttled_pods = 0
    total_throttled_usec = 0
    runqueue_wait_ms = 0.0
    max_nvcswch = 0.0
    cache_miss_not_supported = False

    for pod_data in run["profile"]["per_pod"].values():
        cpu_delta = pod_data.get("cpu_stat_delta", {})
        if cpu_delta.get("nr_throttled", 0) > 0 or cpu_delta.get("throttled_usec", 0) > 0:
            throttled_pods += 1
            total_throttled_usec += cpu_delta.get("throttled_usec", 0)
        sched_delta = pod_data.get("schedstat_delta", {})
        runqueue_wait_ms += float(sched_delta.get("runqueue_wait_ms", 0.0))
        pidstat_summary = pod_data.get("pidstat_summary", {})
        max_nvcswch = max(max_nvcswch, float(pidstat_summary.get("max_nvcswch_per_s", 0.0)))
        perf_summary = pod_data.get("perf_summary", {})
        if perf_summary.get("cache-misses") == "<not supported>":
            cache_miss_not_supported = True

    load = run["profile"]["k8s_snapshot"]["load_result"]
    notes = []
    if cpu_util_pct >= 80 and throttled_pods > 0:
        notes.append("Primary signal: CPU saturation with Kubernetes throttling.")
    elif cpu_util_pct >= 80:
        notes.append("Primary signal: CPU saturation without strong throttling evidence.")
    if total_throttled_usec > 0:
        notes.append("Throttle counters increased during the profiled load window.")
    if runqueue_wait_ms > 100:
        notes.append("Replica threads accumulated noticeable run-queue wait, suggesting scheduling contention.")
    if max_nvcswch > 100:
        notes.append("High involuntary context-switch rate suggests backpressure or scheduler preemption near saturation.")
    if not notes:
        notes.append("No single hard limit dominated; weak scaling likely comes from per-request fan-out plus coordination overhead.")

    memory_values = [row.get("memory_mib") for row in top_rows if isinstance(row.get("memory_mib"), int)]
    if memory_values and max(memory_values) < 128:
        notes.append("Memory stayed low, so memory pressure is unlikely to be the bottleneck.")
    log_errors = [
        line
        for pod_data in run["profile"]["per_pod"].values()
        for line in pod_data.get("log_errors", [])
    ]
    if log_errors:
        notes.append("Application logs show runtime errors worth checking in the raw artifacts.")
    if cache_miss_not_supported:
        notes.append("Cache-miss hardware counters were not available from perf on this node.")
    return " ".join(notes)


def render_plots(summary_rows: List[Dict[str, Any]], latency_rows: List[Dict[str, Any]], output_dir: Path) -> None:
    df_summary = pd.DataFrame(summary_rows).sort_values(["cpu_millicores", "replicas"])
    df_latency = pd.DataFrame(latency_rows).sort_values(["cpu_millicores", "replicas", "effective_rps"])

    plt.figure(figsize=(8, 5))
    for replicas, group in df_summary.groupby("replicas"):
        plt.plot(group["cpu_total_m"], group["msc_rps"], marker="o", label=f"{replicas} replicas")
    plt.xlabel("Total CPU limit (millicores)")
    plt.ylabel("MSC (RPS)")
    plt.title("Rate CPU vs MSC")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / "cpu_vs_msc.png", dpi=160)
    plt.close()

    plt.figure(figsize=(8, 5))
    for key, group in df_latency.groupby(["cpu_millicores", "replicas"]):
        label = f"{key[0]}m x {key[1]}"
        plt.plot(group["effective_rps"], group["p50_latency_ms"], marker="o", label=f"{label} p50")
        plt.plot(group["effective_rps"], group["p90_latency_ms"], marker="s", linestyle="--", label=f"{label} p90")
        plt.plot(group["effective_rps"], group["p99_latency_ms"], marker="^", linestyle=":", label=f"{label} p99")
    plt.xlabel("Effective throughput (successful RPS)")
    plt.ylabel("Latency (ms)")
    plt.title("Rate Latency vs Throughput")
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=8, ncol=2)
    plt.tight_layout()
    plt.savefig(output_dir / "latency_vs_throughput.png", dpi=160)
    plt.close()

    plt.figure(figsize=(8, 5))
    for cpu, group in df_summary.groupby("cpu_millicores"):
        plt.plot(group["replicas"], group["scaling_efficiency"], marker="o", label=f"{cpu}m")
    plt.xlabel("Replicas")
    plt.ylabel("Scaling efficiency")
    plt.title("Rate Replica Scaling Efficiency")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / "replica_scaling_efficiency.png", dpi=160)
    plt.close()

    plt.figure(figsize=(8, 5))
    for replicas, group in df_summary.groupby("replicas"):
        plt.plot(group["cpu_total_m"], group["cpu_efficiency_rps_per_core"], marker="o", label=f"{replicas} replicas")
    plt.xlabel("Total CPU limit (millicores)")
    plt.ylabel("MSC / vCPU")
    plt.title("Rate CPU Efficiency")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / "cpu_efficiency.png", dpi=160)
    plt.close()


def write_report(
    runs: List[Dict[str, Any]],
    summary_rows: List[Dict[str, Any]],
    output_dir: Path,
) -> None:
    lines = ["# Rate Bottleneck Diagnosis", ""]
    lines.append("Investigation date: 2026-05-01")
    lines.append("Scope: `rate` leaf-service reruns for `200m x 2`, `200m x 3`, `500m x 2`, and `500m x 3`.")
    lines.append("Method: direct gRPC load on `rate`, cold-cache steps, `kubectl top`, pod cgroup stats, `perf stat`, and `pidstat -wt`.")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    for row in summary_rows:
        lines.append(
            f"- `{row['config']}`: MSC `{row['msc_rps']}` RPS, profiled load `{row['profile_rps']}` RPS, "
            f"avg CPU `{row['avg_profile_cpu_m']:.1f}m`, diagnosis: {row['diagnosis']}"
        )
    lines.append("")
    lines.append("## Detailed Findings")
    lines.append("")
    for run in runs:
        cfg = run["config"]
        lines.append(f"### {cfg}")
        lines.append("")
        lines.append(f"- MSC search violation reason: `{run['msc']['violation_reason']}`")
        load = run["profile"]["k8s_snapshot"]["load_result"]
        lines.append(
            f"- Profile step: input `{load['rps']}` RPS, success `{load['success_rate']:.3f}`, "
            f"effective `{load['effective_rps']:.1f}` RPS, p50/p90/p99 "
            f"`{load['p50_latency_ms']:.1f}/{load['p90_latency_ms']:.1f}/{load['p99_latency_ms']:.1f}` ms"
        )
        for pod, pod_data in run["profile"]["per_pod"].items():
            cpu_delta = pod_data.get("cpu_stat_delta", {})
            perf_summary = pod_data.get("perf_summary", {})
            pidstat_summary = pod_data.get("pidstat_summary", {})
            sched_delta = pod_data.get("schedstat_delta", {})
            lines.append(
                f"- Pod `{pod}`: throttled periods `{cpu_delta.get('nr_throttled', 0)}`, "
                f"throttled usec `{cpu_delta.get('throttled_usec', 0)}`, "
                f"avg voluntary/involuntary ctx switches per second "
                f"`{pidstat_summary.get('avg_cswch_per_s', 0):.1f}/{pidstat_summary.get('avg_nvcswch_per_s', 0):.1f}`, "
                f"run-queue wait `{sched_delta.get('runqueue_wait_ms', 0):.1f}` ms, "
                f"cycles `{perf_summary.get('cycles', 'n/a')}`, "
                f"instructions `{perf_summary.get('instructions', 'n/a')}`, "
                f"cache misses `{perf_summary.get('cache-misses', 'n/a')}`, "
                f"task migrations `{perf_summary.get('cpu-migrations', 'n/a')}`"
            )
            if pod_data.get("log_errors"):
                lines.append("- Log excerpts:")
                for entry in pod_data["log_errors"][:5]:
                    lines.append(f"  - {entry}")
        lines.append("")
    (output_dir / "bottleneck_report.md").write_text("\n".join(lines))


def run_configuration(
    cpu_millicores: int,
    replicas: int,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    msc = discover_msc(
            replicas=replicas,
            duration_seconds=args.duration,
            timeout_seconds=args.timeout,
            start_rps=args.start_rps,
            step_rps=args.step_rps,
            max_rps=args.max_rps,
            success_threshold=args.success_threshold,
            p90_threshold_ms=args.p90_ms,
            flush_cache_enabled=not args.no_flush_cache,
        )
    probe_loads = build_probe_loads(msc["msc_rps"], args.step_rps, args.max_rps)
    latency_curve = []
    for load in probe_loads:
        if not args.no_flush_cache:
            flush_memcached("rate")
        _, targets, port_forwards = wait_for_rate_targets(replicas)
        try:
            latency_curve.append(
                asdict(run_step_detailed(targets, load, args.duration, args.timeout))
            )
        finally:
            for process in port_forwards:
                process.terminate()
            for process in port_forwards:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)

    profile_rps = min(args.max_rps, max(msc["msc_rps"], probe_loads[-1]))
    pod_names, targets, port_forwards = wait_for_rate_targets(replicas)
    try:
        profile = run_profile_window(
            pod_names=pod_names,
            targets=targets,
            rps=profile_rps,
            duration_seconds=args.duration,
            timeout_seconds=args.timeout,
            flush_cache_enabled=not args.no_flush_cache,
        )
    finally:
        for process in port_forwards:
            process.terminate()
        for process in port_forwards:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
    return {
        "config": f"{cpu_millicores}m x {replicas}",
        "cpu_millicores": cpu_millicores,
        "replicas": replicas,
        "msc": msc,
        "latency_curve": latency_curve,
        "profile": profile,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Focused rate bottleneck investigation.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--start-rps", type=int, default=10)
    parser.add_argument("--step-rps", type=int, default=10)
    parser.add_argument("--duration", type=int, default=15)
    parser.add_argument("--max-rps", type=int, default=300)
    parser.add_argument("--success-threshold", type=float, default=0.99)
    parser.add_argument("--p90-ms", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=0.3)
    parser.add_argument("--no-flush-cache", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ensure_node_tools()
    original = get_current_resources()
    baselines = existing_baseline_msc()
    configs = [(200, 2), (200, 3), (500, 2), (500, 3)]
    runs = []

    try:
        for cpu_millicores, replicas in configs:
            print(f"Running rate investigation for {cpu_millicores}m x {replicas}", flush=True)
            patch_rate_deployment(cpu_millicores, replicas)
            run = run_configuration(cpu_millicores, replicas, args)
            runs.append(run)
            run_dir = output_dir / f"rate-{cpu_millicores}m-{replicas}rep"
            run_dir.mkdir(parents=True, exist_ok=True)
            write_json(run_dir / "run.json", run)
    finally:
        restore_rate_deployment(original)

    summary_rows = []
    latency_rows = []
    for run in runs:
        cpu = run["cpu_millicores"]
        replicas = run["replicas"]
        top_rows = run["profile"]["k8s_snapshot"].get("kubectl_top", [])
        avg_profile_cpu_m = statistics.mean(row["cpu_millicores"] for row in top_rows) if top_rows else 0.0
        baseline = baselines.get(cpu)
        scaling_efficiency = (
            run["msc"]["msc_rps"] / (baseline * replicas) if baseline and baseline > 0 else float("nan")
        )
        cpu_efficiency = run["msc"]["msc_rps"] / ((cpu * replicas) / 1000)
        diagnosis = diagnose_run(run, cpu, replicas)
        summary_rows.append(
            {
                "config": run["config"],
                "cpu_millicores": cpu,
                "replicas": replicas,
                "cpu_total_m": cpu * replicas,
                "msc_rps": run["msc"]["msc_rps"],
                "profile_rps": run["profile"]["rps"],
                "avg_profile_cpu_m": avg_profile_cpu_m,
                "scaling_efficiency": scaling_efficiency,
                "cpu_efficiency_rps_per_core": cpu_efficiency,
                "diagnosis": diagnosis,
            }
        )
        for row in run["latency_curve"]:
            latency_rows.append(
                {
                    "config": run["config"],
                    "cpu_millicores": cpu,
                    "replicas": replicas,
                    **row,
                }
            )

    pd.DataFrame(summary_rows).to_csv(output_dir / "summary.csv", index=False)
    pd.DataFrame(latency_rows).to_csv(output_dir / "latency_curve.csv", index=False)
    render_plots(summary_rows, latency_rows, output_dir)
    write_report(runs, summary_rows, output_dir)
    write_json(output_dir / "investigation.json", {"runs": runs, "summary": summary_rows})


if __name__ == "__main__":
    main()
