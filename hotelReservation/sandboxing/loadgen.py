from __future__ import annotations

import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import grpc

from .proto_codegen import load_modules


@dataclass
class StepResult:
    rps: int
    success_rate: float
    p90_latency_ms: float
    count: int


def run_search_step(
    repo_root: Path,
    target: str,
    requests_corpus: List[Dict[str, Any]],
    rps: int,
    duration_seconds: int,
) -> StepResult:
    build_dir = repo_root / "hotelReservation" / "sandboxing" / "generated"
    proto_module, grpc_module = load_modules(repo_root, build_dir, "search")
    latencies: List[float] = []
    successes = 0
    total = max(1, rps * duration_seconds)
    channel = grpc.insecure_channel(target)
    stub = grpc_module.SearchStub(channel)

    def invoke(payload: Dict[str, Any]) -> bool:
        start = time.perf_counter()
        stub.Nearby(proto_module.NearbyRequest(**payload), timeout=5)
        latencies.append((time.perf_counter() - start) * 1000)
        return True

    try:
        with ThreadPoolExecutor(max_workers=min(32, max(1, rps))) as executor:
            futures = []
            for index in range(total):
                payload = requests_corpus[index % len(requests_corpus)]
                futures.append(executor.submit(invoke, payload))
                if (index + 1) % max(1, rps) == 0:
                    time.sleep(1)
            for future in as_completed(futures):
                try:
                    if future.result():
                        successes += 1
                except Exception:
                    pass
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
    )
