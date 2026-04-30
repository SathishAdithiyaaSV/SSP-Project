#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
SEARCH_CONFIG="${SEARCH_CONFIG:-hotelReservation/sandboxing/examples/search-experiment.json}"
SEARCH_CPU_CONFIGS="${SEARCH_CPU_CONFIGS:-100,200,400,800}"
SEARCH_REPLICA_CONFIGS="${SEARCH_REPLICA_CONFIGS:-1,2,3}"
SEARCH_SKIP_CAPTURE="${SEARCH_SKIP_CAPTURE:-1}"

echo "Running leaf regression collection..."
"$PYTHON_BIN" collect_regression_data.py

echo "Running search regression sweep..."
search_cmd=(
  "$PYTHON_BIN"
  -m
  hotelReservation.sandboxing.experiment
  "$SEARCH_CONFIG"
  --cpu-configs
  "$SEARCH_CPU_CONFIGS"
  --replica-configs
  "$SEARCH_REPLICA_CONFIGS"
)

if [[ "$SEARCH_SKIP_CAPTURE" == "1" ]]; then
  search_cmd+=(--skip-capture)
fi

"${search_cmd[@]}"
