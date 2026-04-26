#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
APP_TAG="${1:-mvp}"

docker build -t deathstarbench/hotel-reservation:${APP_TAG} \
  -f "${ROOT_DIR}/hotelReservation/Dockerfile" \
  "${ROOT_DIR}/hotelReservation"

docker tag deathstarbench/hotel-reservation:${APP_TAG} deathstarbench/hotel-reservation:latest

docker build -t deathstarbench/hotel-reservation-sandbox-dummy:${APP_TAG} \
  -f "${ROOT_DIR}/hotelReservation/sandboxing/Dockerfile.dummy" \
  "${ROOT_DIR}"

docker tag deathstarbench/hotel-reservation-sandbox-dummy:${APP_TAG} deathstarbench/hotel-reservation-sandbox-dummy:latest

minikube image load deathstarbench/hotel-reservation:${APP_TAG}
minikube image load deathstarbench/hotel-reservation:latest
minikube image load deathstarbench/hotel-reservation-sandbox-dummy:${APP_TAG}
minikube image load deathstarbench/hotel-reservation-sandbox-dummy:latest
