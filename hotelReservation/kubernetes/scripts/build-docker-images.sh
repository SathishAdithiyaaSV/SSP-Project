#!/bin/bash

set -e  # exit on error

# Resolve script directory safely
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_FOLDER="$(cd "$SCRIPT_DIR/.." && pwd)"

EXEC="docker buildx"
USER="igorrudyk1"
TAG="latest"

# Create and use builder (ignore error if already exists)
$EXEC create --name mybuilder --use 2>/dev/null || true

for IMAGE in hotelreservation # add more images here if needed
do
  echo "Processing image ${IMAGE}"

  (
    cd "$ROOT_FOLDER" || exit 1

    $EXEC build \
      -t "${USER}/${IMAGE}:${TAG}" \
      -f Dockerfile . \
      --platform linux/arm64,linux/amd64 \
      --push
  )

  echo
done