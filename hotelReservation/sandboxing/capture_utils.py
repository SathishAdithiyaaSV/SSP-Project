from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 4)
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalize_json_value(item) for key, item in value.items()}
    return value


def canonical_json(payload: Dict[str, Any]) -> str:
    return json.dumps(_normalize_json_value(payload), separators=(",", ":"), sort_keys=True)


def load_ndjson(path: str | Path) -> List[Dict[str, Any]]:
    records = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def normalize_capture(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    fixtures: List[Dict[str, Any]] = []
    seen = set()
    for record in records:
        request = record.get("request") or {}
        response = record.get("response") or {}
        if isinstance(request, str):
            request = json.loads(request)
        if isinstance(response, str):
            response = json.loads(response)
        key = (record["method"], canonical_json(request))
        if key in seen:
            continue
        seen.add(key)
        fixtures.append(
            {
                "method": record["method"],
                "request": request,
                "response": response,
                "code": record["code"],
            }
        )
    return {"fixtures": fixtures}


def extract_request_corpus(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    corpus = []
    seen = set()
    for record in records:
        request = record.get("request") or {}
        if isinstance(request, str):
            request = json.loads(request)
        key = canonical_json(request)
        if key in seen:
            continue
        seen.add(key)
        corpus.append(request)
    return {"requests": corpus}
