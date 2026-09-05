# Sandbox-Based Capacity Characterization of Hotel Reservation

This project measures **Microservice Capacity (MSC)** for the
DeathStarBench Hotel Reservation application. It is a modification of the
original benchmark that adds the sandbox-based capacity-characterization
workflow and the experiment, analysis, and reporting artifacts in this
repository. MSC is the highest offered request rate that satisfies both the
success-rate and P90-latency SLO. The study measures `search` in a
replay-based dependency sandbox and directly measures the leaf services
`geo`, `rate`, `profile`, `recommendation`, `user`, and `reservation`.

The final project report is [docs/report/Report.pdf](docs/report/Report.pdf).
It describes the methodology, data, results, predictive model, and rate
bottleneck investigation in detail.

## Repository layout

| Path | Purpose |
| --- | --- |
| `hotelReservation/` | Hotel Reservation benchmark application, deployment manifests, and sandbox implementation. |
| `scripts/` | Top-level repeatable experiment runners. |
| `analysis/` | Reproducible plotting code. |
| `data/measurements/` | Versioned CSV snapshots used for the reported analysis. |
| `docs/report/` | The final report. |
| `assets/` | Small, versioned project assets. |
| `artifacts/` | Generated captures, datasets, models, charts, and PDFs; intentionally ignored by Git. |

## Requirements

- Docker and a local Kubernetes cluster (the sandbox workflow is tested with
  Minikube)
- `kubectl`, `minikube`, and Python 3.11+
- Go/Docker build tooling required by the Hotel Reservation image
- A TeX distribution with `pdflatex` to build the report (optional)

Start Minikube and enable metrics before running experiments:

```bash
minikube start
minikube addons enable metrics-server
```

Create a Python environment for the sandbox tooling:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r hotelReservation/sandboxing/requirements.txt pandas matplotlib
```

## Reproduce the report from scratch

The full workflow changes live Kubernetes deployment CPU limits and replica
counts, but each leaf-service run saves and restores the original deployment
configuration. Use a disposable local cluster, not a shared production one.

### 1. Build and deploy the baseline application

```bash
bash hotelReservation/sandboxing/build_images.sh
kubectl apply -Rf hotelReservation/kubernetes/
kubectl get pods
```

Wait for the baseline pods to become ready. Ensure that `geo`, `rate`, and
`search` use the locally built image, then wait for their rollouts:

```bash
kubectl set image deployment/geo hotel-reserv-geo=deathstarbench/hotel-reservation:mvp
kubectl set image deployment/rate hotel-reserv-rate=deathstarbench/hotel-reservation:mvp
kubectl set image deployment/search hotel-reserv-search=deathstarbench/hotel-reservation:mvp
kubectl rollout status deployment/geo
kubectl rollout status deployment/rate
kubectl rollout status deployment/search
```

In a separate terminal, leave the frontend port-forward running for capture:

```bash
kubectl port-forward svc/frontend 8080:5000
```

### 2. Measure sandboxed `search`

The report uses CPU values `100,200,500` millicores and replica counts `1,2,3`.
The command captures real baseline traffic, creates replay fixtures, deploys a
fresh sandbox, and writes results beneath `artifacts/experiments/search/`.

```bash
python -m hotelReservation.sandboxing.experiment \
  hotelReservation/sandboxing/examples/search-experiment.json \
  --cpu-configs 100,200,500 \
  --replica-configs 1,2,3
```

For a measurement-only rerun, reuse captures with `--skip-capture`; add
`--skip-apply` only when the sandbox is already deployed. The detailed
[sandbox runbook](hotelReservation/sandboxing/RUNBOOK.md) includes diagnostics
and common failure modes.

### 3. Measure the leaf services

Run the report grid (100m, 200m, 500m × 1, 2, 3 replicas):

```bash
python scripts/collect_regression_data.py
```

The output is `artifacts/experiments/leaf-regression/dataset.csv`. To run the
held-out 150m validation mentioned in the report, use:

```bash
LEAF_CPU_CONFIGS=150 LEAF_REPLICA_CONFIGS=1,2 \
  python scripts/collect_regression_data.py
```

`scripts/run_leaf_then_search.sh` runs both collection paths; it accepts the
same `LEAF_*` variables plus `SEARCH_CPU_CONFIGS`, `SEARCH_REPLICA_CONFIGS`,
and `SEARCH_SKIP_CAPTURE`.

### 4. Generate figures and the report

The versioned CSV snapshots in `data/measurements/` are the inputs used for the
checked-in report analysis. Regenerate its standard plots with:

```bash
python analysis/plot_performance.py --output artifacts/report-figures
```

The two rate-investigation figures require the rate investigation workload:

```bash
python -m hotelReservation.sandboxing.rate_bottleneck_investigation \
  --output-dir artifacts/report-figures
```

## Fast, no-cluster verification

To regenerate the standard charts from the supplied measurement snapshots:

```bash
python analysis/plot_performance.py
```

## Notes on results

The reported SLO is success rate ≥ 0.99 and P90 latency ≤ 100 ms. The report
finds that dependency sandboxing exposes substantially more `search` capacity
than live dependency measurements, while `rate` is the weakest observed leaf
service. Capacity values are environment-sensitive: Docker/Minikube versions,
host CPU, warm-up, and cluster load can change new measurements. Treat the
versioned CSVs as the exact analysis snapshot for the submitted report.

## License and attribution

This repository contains and modifies the DeathStarBench Hotel Reservation
benchmark. It is distributed under the Apache License 2.0; see
[LICENSE](LICENSE). Please cite the original DeathStarBench publication when
using the benchmark.
