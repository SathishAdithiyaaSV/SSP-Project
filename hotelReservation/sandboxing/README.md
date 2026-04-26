# Hotel Reservation Sandboxing MVP

This package implements a Minikube-first MVP of the paper workflow for
Hotel Reservation, starting with the `search` service.

For a full fresh-machine setup and repeatable run procedure, use
[RUNBOOK.md](./RUNBOOK.md).

## What it does

- captures real baseline gRPC traffic for `search`, `geo`, and `rate`
- normalizes captured request/response pairs into replay fixtures
- renders a sandbox namespace where `search` talks to dummy `geo` and `rate`
- runs increasing-load tests against sandboxed `search`
- computes MSC from success-rate and P90 latency thresholds
- saves a small regression model artifact for later capacity estimation

## Current v1 scope

- fully supported target: `search`
- paper-style dependency sandboxing only
- Minikube only
- default SLO: success rate `>= 0.98`, P90 latency `<= 3000 ms`

## Typical flow

1. Build and load the updated Hotel Reservation image.
2. Re-deploy baseline Hotel Reservation on Minikube.
3. Install Python requirements from `requirements.txt`.
4. Run:

```bash
python3 -m hotelReservation.sandboxing.experiment \
  hotelReservation/sandboxing/examples/search-experiment.json
```

Artifacts are written under the configured `output_dir`.
