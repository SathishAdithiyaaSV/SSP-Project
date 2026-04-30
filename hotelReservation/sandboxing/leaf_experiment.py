from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import socket
import statistics
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import grpc
from grpc_tools import protoc


REPO_ROOT = Path(__file__).resolve().parents[2]
HOTEL_ROOT = REPO_ROOT / "hotelReservation"
BUILD_DIR = HOTEL_ROOT / "sandboxing" / "generated_leaf"

# Maps service name to its Memcached deployment name.
# Only services that use Memcached are listed here.
MEMCACHED_MAP: Dict[str, str] = {
    "rate": "memcached-rate",
    "profile": "memcached-profile",
    "reservation": "memcached-reserve",
}


@dataclass(frozen=True)
class ServiceSpec:
    name: str
    deployment: str
    label_selector: str
    port: int
    proto_path: str
    stub_class: str
    method_name: str
    request_class: str
    payloads: List[Dict[str, Any]]


@dataclass
class StepResult:
    rps: int
    success_rate: float
    p90_latency_ms: float
    count: int
    failures: int
    error_summary: List[Dict[str, Any]]


# ---------------------------------------------------------------------------
# Payload helpers
# ---------------------------------------------------------------------------

def _load_hotel_ids() -> List[str]:
    """Load hotel IDs from the static dataset."""
    hotels_path = HOTEL_ROOT / "data" / "hotels.json"
    hotels = json.loads(hotels_path.read_text())
    return [str(h["id"]) for h in hotels if "id" in h]


def _make_profile_payloads() -> List[Dict[str, Any]]:
    ids = _load_hotel_ids()
    payloads = []
    # Single hotel queries + full batch to vary request size
    for hotel_id in ids:
        payloads.append({"hotelIds": [hotel_id], "locale": "en"})
    payloads.append({"hotelIds": ids, "locale": "en"})
    return payloads


def _make_rate_payloads() -> List[Dict[str, Any]]:
    ids = _load_hotel_ids()
    date_windows = [
        ("2015-04-09", "2015-04-10"),
        ("2015-04-17", "2015-04-18"),
        ("2015-04-20", "2015-04-24"),
    ]
    payloads = []
    for hotel_id in ids:
        for in_date, out_date in date_windows:
            payloads.append({
                "hotelIds": [hotel_id],
                "inDate": in_date,
                "outDate": out_date,
            })
    # Also add full-batch requests
    for in_date, out_date in date_windows:
        payloads.append({
            "hotelIds": ids,
            "inDate": in_date,
            "outDate": out_date,
        })
    return payloads


def _make_reservation_payloads() -> List[Dict[str, Any]]:
    ids = _load_hotel_ids()
    date_windows = [
        ("2015-04-09", "2015-04-10"),
        ("2015-04-17", "2015-04-18"),
    ]
    payloads = []
    for hotel_id in ids:
        for in_date, out_date in date_windows:
            payloads.append({
                "customerName": "",
                "hotelId": [hotel_id],
                "inDate": in_date,
                "outDate": out_date,
                "roomNumber": 1,
            })
    return payloads


def _make_user_payloads() -> List[Dict[str, Any]]:
    """Generate varied user check payloads using the seeded Cornell users."""
    return [
        {
            "username": f"Cornell_{i}",
            "password": hashlib.sha256(str(i).encode()).hexdigest(),
        }
        for i in range(500)
    ]


SERVICES: Dict[str, ServiceSpec] = {
    "geo": ServiceSpec(
        name="geo",
        deployment="geo",
        label_selector="io.kompose.service=geo",
        port=8083,
        proto_path="services/geo/proto/geo.proto",
        stub_class="GeoStub",
        method_name="Nearby",
        request_class="Request",
        payloads=[
            {"lat": 37.7749, "lon": -122.4194},
            {"lat": 38.0235, "lon": -122.095},
            {"lat": 37.3861, "lon": -122.0839},
            {"lat": 37.5630, "lon": -122.0530},
            {"lat": 37.6879, "lon": -122.4702},
            {"lat": 37.8044, "lon": -122.2712},
        ],
    ),
    "rate": ServiceSpec(
        name="rate",
        deployment="rate",
        label_selector="io.kompose.service=rate",
        port=8084,
        proto_path="services/rate/proto/rate.proto",
        stub_class="RateStub",
        method_name="GetRates",
        request_class="Request",
        payloads=_make_rate_payloads(),
    ),
    "profile": ServiceSpec(
        name="profile",
        deployment="profile",
        label_selector="io.kompose.service=profile",
        port=8081,
        proto_path="services/profile/proto/profile.proto",
        stub_class="ProfileStub",
        method_name="GetProfiles",
        request_class="Request",
        payloads=_make_profile_payloads(),
    ),
    "recommendation": ServiceSpec(
        name="recommendation",
        deployment="recommendation",
        label_selector="io.kompose.service=recommendation",
        port=8085,
        proto_path="services/recommendation/proto/recommendation.proto",
        stub_class="RecommendationStub",
        method_name="GetRecommendations",
        request_class="Request",
        payloads=[
            {"require": "price", "lat": 37.7749, "lon": -122.4194},
            {"require": "rate",  "lat": 37.7749, "lon": -122.4194},
            {"require": "dis",   "lat": 37.7749, "lon": -122.4194},
            {"require": "price", "lat": 38.0235, "lon": -122.095},
            {"require": "rate",  "lat": 38.0235, "lon": -122.095},
            {"require": "dis",   "lat": 38.0235, "lon": -122.095},
        ],
    ),
    "user": ServiceSpec(
        name="user",
        deployment="user",
        label_selector="io.kompose.service=user",
        port=8086,
        proto_path="services/user/proto/user.proto",
        stub_class="UserStub",
        method_name="CheckUser",
        request_class="Request",
        payloads=_make_user_payloads(),
    ),
    "reservation": ServiceSpec(
        name="reservation",
        deployment="reservation",
        label_selector="io.kompose.service=reservation",
        port=8087,
        proto_path="services/reservation/proto/reservation.proto",
        stub_class="ReservationStub",
        method_name="CheckAvailability",
        request_class="Request",
        payloads=_make_reservation_payloads(),
    ),
}


# ---------------------------------------------------------------------------
# Kubernetes helpers
# ---------------------------------------------------------------------------

def run_command(args: List[str], **kwargs: Any) -> subprocess.CompletedProcess:
    return subprocess.run(args, check=True, text=True, **kwargs)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def flush_memcached(service_name: str, namespace: str = "default") -> None:
    """
    Flush Memcached for a service to ensure each load step starts cold.
    Uses a disposable busybox pod — no nc binary required in the target container.
    Silently skips services that have no Memcached dependency.
    """
    memc_deployment = MEMCACHED_MAP.get(service_name)
    if not memc_deployment:
        return

    # Resolve the ClusterIP of the Memcached service so busybox can reach it
    try:
        result = subprocess.run(
            [
                "kubectl", "get", "service", memc_deployment,
                "-n", namespace,
                "-o", "jsonpath={.spec.clusterIP}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        cluster_ip = result.stdout.strip()
        if not cluster_ip:
            return
    except subprocess.CalledProcessError:
        return

    try:
        subprocess.run(
            [
                "kubectl", "run",
                f"memc-flush-{service_name}",
                "--rm", "--restart=Never",
                "--image=busybox",
                f"--namespace={namespace}",
                "--command",
                "--",
                "sh", "-c",
                f"echo flush_all | nc {cluster_ip} 11211",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        # Non-fatal — experiment continues even if flush fails
        pass


# ---------------------------------------------------------------------------
# Proto codegen
# ---------------------------------------------------------------------------

def ensure_generated() -> None:
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    if str(BUILD_DIR) not in sys.path:
        sys.path.insert(0, str(BUILD_DIR))

    for spec in SERVICES.values():
        proto_path = HOTEL_ROOT / spec.proto_path
        result = protoc.main(
            [
                "grpc_tools.protoc",
                f"-I{HOTEL_ROOT}",
                f"--python_out={BUILD_DIR}",
                f"--grpc_python_out={BUILD_DIR}",
                str(proto_path),
            ]
        )
        if result != 0:
            raise RuntimeError(f"failed to compile proto: {proto_path}")


def load_service_modules(spec: ServiceSpec) -> Tuple[object, object]:
    ensure_generated()
    proto_module = importlib.import_module(f"services.{spec.name}.proto.{spec.name}_pb2")
    grpc_module = importlib.import_module(f"services.{spec.name}.proto.{spec.name}_pb2_grpc")
    return proto_module, grpc_module


# ---------------------------------------------------------------------------
# Port-forward helpers
# ---------------------------------------------------------------------------

def reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_port_forward(
    process: subprocess.Popen, local_port: int, timeout_seconds: int = 15
) -> None:
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


# ---------------------------------------------------------------------------
# Load generation
# ---------------------------------------------------------------------------

def summarize_error(exc: Exception) -> str:
    if isinstance(exc, grpc.RpcError):
        code = exc.code()
        code_name = code.name if code is not None else "UNKNOWN_CODE"
        details = exc.details() or ""
        return f"{code_name}: {details}".strip()
    return f"{type(exc).__name__}: {exc}".strip()


def run_step(
    spec: ServiceSpec,
    target: str,
    rps: int,
    duration_seconds: int,
    timeout_seconds: float,
) -> StepResult:
    proto_module, grpc_module = load_service_modules(spec)
    channel = grpc.insecure_channel(target)
    stub = getattr(grpc_module, spec.stub_class)(channel)
    method: Callable[..., Any] = getattr(stub, spec.method_name)
    request_cls = getattr(proto_module, spec.request_class)
    total = max(1, rps * duration_seconds)
    latencies: List[float] = []
    successes = 0
    errors: Counter[str] = Counter()

    def invoke(payload: Dict[str, Any]) -> float:
        request = request_cls(**payload)
        start = time.perf_counter()
        method(request, timeout=timeout_seconds, wait_for_ready=True)
        return (time.perf_counter() - start) * 1000

    try:
        grpc.channel_ready_future(channel).result(timeout=10)
        # Warm the channel before the measured interval
        invoke(spec.payloads[0])
        with ThreadPoolExecutor(max_workers=min(200, max(1, rps))) as executor:
            futures = []
            for index in range(total):
                payload = spec.payloads[index % len(spec.payloads)]
                futures.append(executor.submit(invoke, payload))
                if (index + 1) % max(1, rps) == 0:
                    time.sleep(1)
            for future in as_completed(futures):
                try:
                    latencies.append(future.result())
                    successes += 1
                except Exception as exc:
                    errors[summarize_error(exc)] += 1
    finally:
        channel.close()

    if not latencies:
        p90 = float("inf")
    elif len(latencies) == 1:
        p90 = latencies[0]
    else:
        p90 = statistics.quantiles(latencies, n=10)[-1]

    return StepResult(
        rps=rps,
        success_rate=successes / total,
        p90_latency_ms=p90,
        count=total,
        failures=total - successes,
        error_summary=[
            {"error": error, "count": count}
            for error, count in errors.most_common()
        ],
    )


# ---------------------------------------------------------------------------
# CPU sampling
# ---------------------------------------------------------------------------

def sample_cpu(namespace: str, label_selector: str) -> Tuple[Optional[int], Optional[str]]:
    try:
        result = run_command(
            ["kubectl", "top", "pod", "-n", namespace, "-l", label_selector, "--no-headers"],
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        return None, str(exc)

    total = 0
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        cpu = parts[1]
        if cpu.endswith("m"):
            total += int(cpu[:-1])
        else:
            total += int(float(cpu) * 1000)
    return total, None


# ---------------------------------------------------------------------------
# Experiment runner
# ---------------------------------------------------------------------------

def run_service(
    spec: ServiceSpec,
    namespace: str,
    start_rps: int,
    step_rps: int,
    duration_seconds: int,
    max_rps: int,
    success_threshold: float,
    p90_threshold_ms: int,
    output_dir: Path,
    timeout_seconds: float,
    flush_cache: bool,
) -> Dict[str, Any]:
    local_port = reserve_local_port()
    target = f"127.0.0.1:{local_port}"
    port_forward = subprocess.Popen(
        [
            "kubectl", "port-forward",
            "-n", namespace,
            f"deployment/{spec.deployment}",
            f"{local_port}:{spec.port}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

    steps = []
    best_rps = 0
    violation_reason = ""
    observed_cpu_values: List[int] = []
    metric_errors: List[str] = []

    try:
        wait_for_port_forward(port_forward, local_port)

        for rps in range(start_rps, max_rps + 1, step_rps):

            # Flush Memcached before each step so every request hits MongoDB.
            # This gives us cold-cache MSC — more conservative and meaningful
            # for capacity planning than warm-cache measurements.
            if flush_cache:
                flush_memcached(spec.name, namespace)

            result = run_step(spec, target, rps, duration_seconds, timeout_seconds)
            observed_cpu, metric_error = sample_cpu(namespace, spec.label_selector)

            if observed_cpu is not None:
                observed_cpu_values.append(observed_cpu)
            elif metric_error:
                metric_errors.append(metric_error)

            row = {
                "rps": result.rps,
                "success_rate": result.success_rate,
                "p90_latency_ms": result.p90_latency_ms,
                "count": result.count,
                "failures": result.failures,
                "error_summary": result.error_summary,
                "observed_cpu_millicores": observed_cpu,
                "cache_flushed": flush_cache and spec.name in MEMCACHED_MAP,
            }
            steps.append(row)

            if (result.success_rate >= success_threshold
                    and result.p90_latency_ms <= p90_threshold_ms):
                best_rps = result.rps
                continue

            violation_reason = (
                "success_rate"
                if result.success_rate < success_threshold
                else "p90_latency"
            )
            break

    finally:
        port_forward.terminate()
        try:
            port_forward.wait(timeout=10)
        except subprocess.TimeoutExpired:
            port_forward.kill()
            port_forward.wait(timeout=10)

    payload = {
        "service": spec.name,
        "deployment": spec.deployment,
        "port": spec.port,
        "method": spec.method_name,
        "namespace": namespace,
        "cache_flush_enabled": flush_cache,
        "slo": {
            "success_rate_threshold": success_threshold,
            "p90_latency_ms": p90_threshold_ms,
        },
        "load": {
            "start_rps": start_rps,
            "step_rps": step_rps,
            "step_duration_seconds": duration_seconds,
            "max_rps": max_rps,
        },
        "steps": steps,
        "msc_rps": best_rps,
        "violation_reason": violation_reason or "max_rps_reached",
        "observed_cpu_millicores": max(observed_cpu_values, default=None),
    }
    if metric_errors:
        payload["metric_errors"] = metric_errors[:3]

    write_json(output_dir / "results" / f"{spec.name}-run.json", payload)
    return payload


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure MSC for leaf Hotel Reservation services."
    )
    parser.add_argument(
        "--service",
        choices=sorted(SERVICES),
        help="Run one service; default runs all leaf services.",
    )
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--output-dir", default="sandboxing/output/leaf")
    parser.add_argument("--start-rps", type=int, default=10)
    parser.add_argument("--step-rps", type=int, default=10)
    parser.add_argument("--duration", type=int, default=15)
    parser.add_argument("--max-rps", type=int, default=500)
    parser.add_argument("--success-threshold", type=float, default=0.95)
    parser.add_argument("--p90-ms", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=0.2)
    parser.add_argument(
        "--no-flush-cache",
        action="store_true",
        help="Disable Memcached flush between steps (warm-cache measurement).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    flush_cache = not args.no_flush_cache
    selected = [SERVICES[args.service]] if args.service else list(SERVICES.values())
    results = []

    if flush_cache:
        print("Cache flush enabled: each step starts cold (MongoDB path).", flush=True)
    else:
        print("Cache flush disabled: warm-cache measurement.", flush=True)

    for spec in selected:
        print(
            f"Running {spec.name} on deployment/{spec.deployment}:{spec.port}",
            flush=True,
        )
        result = run_service(
            spec=spec,
            namespace=args.namespace,
            start_rps=args.start_rps,
            step_rps=args.step_rps,
            duration_seconds=args.duration,
            max_rps=args.max_rps,
            success_threshold=args.success_threshold,
            p90_threshold_ms=args.p90_ms,
            output_dir=output_dir,
            timeout_seconds=args.timeout,
            flush_cache=flush_cache,
        )
        results.append(result)
        print(
            f"{spec.name}: msc_rps={result['msc_rps']} "
            f"violation={result['violation_reason']}",
            flush=True,
        )

    summary = {
        "cache_flush_enabled": flush_cache,
        "services": [
            {
                "service": item["service"],
                "msc_rps": item["msc_rps"],
                "violation_reason": item["violation_reason"],
                "observed_cpu_millicores": item["observed_cpu_millicores"],
            }
            for item in results
        ],
    }
    write_json(output_dir / "results" / "leaf-summary.json", summary)


if __name__ == "__main__":
    main()