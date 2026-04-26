from __future__ import annotations

import argparse
import json
import os
import socket
import time
from concurrent import futures
from pathlib import Path
from typing import Any, Dict

import grpc
import requests

from .capture_utils import canonical_json
from .proto_codegen import load_modules


class FixtureLookup:
    def __init__(self, fixture_path: Path):
        payload = json.loads(fixture_path.read_text())
        self.fixtures = {}
        for item in payload["fixtures"]:
            key = (item["method"], canonical_json(item["request"]))
            self.fixtures[key] = item

    def get(self, method: str, request: Dict[str, Any]) -> Dict[str, Any]:
        key = (method, canonical_json(request))
        if key not in self.fixtures:
            raise KeyError(f"no fixture for {method} {key[1]}")
        return self.fixtures[key]


class GeoServicer:
    def __init__(self, lookup: FixtureLookup, proto_module: object):
        self.lookup = lookup
        self.proto = proto_module

    def Nearby(self, request: Any, context: grpc.ServicerContext) -> Any:
        req_dict = {"lat": request.lat, "lon": request.lon}
        fixture = self.lookup.get("/geo.Geo/Nearby", req_dict)
        return self.proto.Result(**fixture["response"])


class RateServicer:
    def __init__(self, lookup: FixtureLookup, proto_module: object):
        self.lookup = lookup
        self.proto = proto_module

    def GetRates(self, request: Any, context: grpc.ServicerContext) -> Any:
        req_dict = {
            "hotelIds": list(request.hotelIds),
            "inDate": request.inDate,
            "outDate": request.outDate,
        }
        fixture = self.lookup.get("/rate.Rate/GetRates", req_dict)
        return self.proto.Result(**fixture["response"])


def register_with_consul(consul_addr: str, service_name: str, service_id: str, host: str, port: int) -> None:
    payload = {
        "ID": service_id,
        "Name": service_name,
        "Address": host,
        "Port": port,
    }
    response = requests.put(f"http://{consul_addr}/v1/agent/service/register", json=payload, timeout=5)
    response.raise_for_status()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--service", required=True, choices=["geo", "rate"])
    parser.add_argument("--fixture-path", required=True)
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--consul-addr", required=True)
    parser.add_argument("--consul-name", required=True)
    parser.add_argument("--pod-ip", default="")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    build_dir = repo_root / "hotelReservation" / "sandboxing" / "generated"
    proto_module, grpc_module = load_modules(repo_root, build_dir, args.service)
    lookup = FixtureLookup(Path(args.fixture_path))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    if args.service == "geo":
        grpc_module.add_GeoServicer_to_server(GeoServicer(lookup, proto_module), server)
    elif args.service == "rate":
        grpc_module.add_RateServicer_to_server(RateServicer(lookup, proto_module), server)

    server.add_insecure_port(f"{args.bind}:{args.port}")
    server.start()

    host = args.pod_ip or os.getenv("POD_IP", "") or socket.gethostbyname(socket.gethostname())
    register_with_consul(
        consul_addr=args.consul_addr,
        service_name=args.consul_name,
        service_id=f"{args.consul_name}-dummy-{int(time.time())}",
        host=host,
        port=args.port,
    )
    server.wait_for_termination()


if __name__ == "__main__":
    main()
