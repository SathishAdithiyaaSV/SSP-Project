from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class SLOConfig:
    success_rate_threshold: float = 0.98
    p90_latency_ms: int = 3000


@dataclass
class LoadConfig:
    start_rps: int
    step_rps: int
    step_duration_seconds: int
    max_rps: int


@dataclass
class CaptureConfig:
    warmup_requests: int
    frontend_base_url: str


@dataclass
class ExperimentConfig:
    service: str
    namespace: str
    image: str
    dummy_image: str
    cpu_millicores: int
    memory_mb: int
    replicas: int
    slo: SLOConfig
    load: LoadConfig
    capture: CaptureConfig
    output_dir: Path

    @classmethod
    def from_file(cls, path: str | Path) -> "ExperimentConfig":
        raw = json.loads(Path(path).read_text())
        return cls(
            service=raw["service"],
            namespace=raw["namespace"],
            image=raw["image"],
            dummy_image=raw["dummy_image"],
            cpu_millicores=int(raw["cpu_millicores"]),
            memory_mb=int(raw["memory_mb"]),
            replicas=int(raw["replicas"]),
            slo=SLOConfig(**raw["slo"]),
            load=LoadConfig(**raw["load"]),
            capture=CaptureConfig(**raw["capture"]),
            output_dir=Path(raw["output_dir"]),
        )


def write_json(path: str | Path, payload: Any) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True))
