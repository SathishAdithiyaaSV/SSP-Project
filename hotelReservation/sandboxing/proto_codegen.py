from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Tuple

from grpc_tools import protoc


PROTO_FILES = {
    "search": "services/search/proto/search.proto",
    "geo": "services/geo/proto/geo.proto",
    "rate": "services/rate/proto/rate.proto",
}


def ensure_generated(repo_root: Path, build_dir: Path) -> Path:
    build_dir.mkdir(parents=True, exist_ok=True)
    stamp = build_dir / ".generated"
    if stamp.exists():
        _ensure_import_path(build_dir)
        return build_dir

    for proto in PROTO_FILES.values():
        proto_path = repo_root / "hotelReservation" / proto
        result = protoc.main(
            [
                "grpc_tools.protoc",
                f"-I{repo_root / 'hotelReservation'}",
                f"--python_out={build_dir}",
                f"--grpc_python_out={build_dir}",
                str(proto_path),
            ]
        )
        if result != 0:
            raise RuntimeError(f"failed to compile proto: {proto_path}")

    stamp.write_text("ok")
    _ensure_import_path(build_dir)
    return build_dir


def load_modules(repo_root: Path, build_dir: Path, service: str) -> Tuple[object, object]:
    ensure_generated(repo_root, build_dir)
    proto = importlib.import_module(f"services.{service}.proto.{service}_pb2")
    grpc_module = importlib.import_module(f"services.{service}.proto.{service}_pb2_grpc")
    return proto, grpc_module


def _ensure_import_path(build_dir: Path) -> None:
    if str(build_dir) not in sys.path:
        sys.path.insert(0, str(build_dir))
