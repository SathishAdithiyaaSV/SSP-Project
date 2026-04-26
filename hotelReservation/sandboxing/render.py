from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, Dict, List

import yaml

from .config import ExperimentConfig


def _configmap(name: str, namespace: str, filename: str, content: str) -> Dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": namespace},
        "data": {filename: content},
    }


def render_search_sandbox(
    config: ExperimentConfig,
    repo_root: Path,
    geo_fixture_path: Path,
    rate_fixture_path: Path,
) -> List[Dict[str, Any]]:
    namespace = config.namespace
    geo_fixture = geo_fixture_path.read_text()
    rate_fixture = rate_fixture_path.read_text()
    repo_root_str = "/workspace"

    documents: List[Dict[str, Any]] = [
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": namespace},
        },
        _configmap("geo-fixtures", namespace, "geo.json", geo_fixture),
        _configmap("rate-fixtures", namespace, "rate.json", rate_fixture),
        _consul(namespace),
        _service(namespace, "consul", 8500),
        _jaeger(namespace),
        _service(namespace, "jaeger", 6831, protocol="UDP"),
        _dummy_service(
            namespace=namespace,
            service="geo",
            port=8083,
            consul_name="srv-geo",
            fixture_configmap="geo-fixtures",
            fixture_filename="geo.json",
            image=config.dummy_image,
            repo_root=repo_root_str,
        ),
        _dummy_service(
            namespace=namespace,
            service="rate",
            port=8084,
            consul_name="srv-rate",
            fixture_configmap="rate-fixtures",
            fixture_filename="rate.json",
            image=config.dummy_image,
            repo_root=repo_root_str,
        ),
        _search_service(config),
    ]
    return documents


def write_manifest(path: Path, documents: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump_all(documents, handle, sort_keys=False)


def _consul(namespace: str) -> Dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "consul", "namespace": namespace},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "consul"}},
            "template": {
                "metadata": {"labels": {"app": "consul"}},
                "spec": {
                    "containers": [
                        {
                            "name": "consul",
                            "image": "hashicorp/consul:latest",
                            "args": ["agent", "-dev", "-client", "0.0.0.0"],
                            "ports": [{"containerPort": 8500}],
                        }
                    ]
                },
            },
        },
    }


def _jaeger(namespace: str) -> Dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "jaeger", "namespace": namespace},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "jaeger"}},
            "template": {
                "metadata": {"labels": {"app": "jaeger"}},
                "spec": {
                    "containers": [
                        {
                            "name": "jaeger",
                            "image": "jaegertracing/all-in-one:latest",
                            "ports": [
                                {"containerPort": 16686},
                                {"containerPort": 6831, "protocol": "UDP"},
                            ],
                        }
                    ]
                },
            },
        },
    }


def _dummy_service(
    namespace: str,
    service: str,
    port: int,
    consul_name: str,
    fixture_configmap: str,
    fixture_filename: str,
    image: str,
    repo_root: str,
) -> Dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": service, "namespace": namespace},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": service}},
            "template": {
                "metadata": {"labels": {"app": service}},
                "spec": {
                    "containers": [
                        {
                            "name": f"{service}-dummy",
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["python", "-m", "hotelReservation.sandboxing.dummy_server"],
                            "args": [
                                "--repo-root",
                                repo_root,
                                "--service",
                                service,
                                "--fixture-path",
                                f"/fixtures/{fixture_filename}",
                                "--port",
                                str(port),
                                "--consul-addr",
                                "consul:8500",
                                "--consul-name",
                                consul_name,
                            ],
                            "env": [
                                {
                                    "name": "POD_IP",
                                    "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}},
                                }
                            ],
                            "ports": [{"containerPort": port}],
                            "volumeMounts": [
                                {"name": "fixtures", "mountPath": "/fixtures", "readOnly": True}
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "fixtures", "configMap": {"name": fixture_configmap}}
                    ],
                },
            },
        },
    }


def _service(namespace: str, name: str, port: int, protocol: str = "TCP") -> Dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "selector": {"app": name},
            "ports": [{"port": port, "targetPort": port, "protocol": protocol}],
        },
    }


def _search_service(config: ExperimentConfig) -> Dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "search", "namespace": config.namespace},
        "spec": {
            "replicas": config.replicas,
            "selector": {"matchLabels": {"app": "search"}},
            "template": {
                "metadata": {"labels": {"app": "search"}},
                "spec": {
                    "containers": [
                        {
                            "name": "search",
                            "image": config.image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/go/bin/search"],
                            "resources": {
                                "requests": {
                                    "cpu": f"{config.cpu_millicores}m",
                                    "memory": f"{config.memory_mb}Mi",
                                },
                                "limits": {
                                    "cpu": f"{config.cpu_millicores}m",
                                    "memory": f"{config.memory_mb}Mi",
                                },
                            },
                            "ports": [{"containerPort": 8082}],
                        }
                    ]
                },
            },
        },
    }
