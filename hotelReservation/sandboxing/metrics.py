from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass
from typing import List


@dataclass
class PodMetric:
    pod: str
    cpu_millicores: int


def sample_cpu(namespace: str, label_selector: str) -> List[PodMetric]:
    last_error = None
    for _ in range(12):
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "top",
                    "pod",
                    "-n",
                    namespace,
                    "-l",
                    label_selector,
                    "--no-headers",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            break
        except subprocess.CalledProcessError as exc:
            last_error = exc
            stderr = (exc.stderr or "").lower()
            if "metrics not available yet" not in stderr:
                raise
            time.sleep(5)
    else:
        raise RuntimeError(
            f"timed out waiting for pod metrics in namespace {namespace} with selector {label_selector}"
        ) from last_error

    metrics = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        cpu = parts[1]
        if cpu.endswith("m"):
            value = int(cpu[:-1])
        else:
            value = int(float(cpu) * 1000)
        metrics.append(PodMetric(pod=parts[0], cpu_millicores=value))
    return metrics
