# Hotel Reservation Sandboxing Runbook

This runbook is the shortest reliable path to get the Hotel Reservation
sandboxing experiment working on a new system.

It assumes you are running from the repository root:

```bash
cd /path/to/DeathStarBench
```

## What this workflow does

The experiment:

1. sends warmup traffic through the baseline Hotel Reservation app
2. captures real gRPC traffic from `geo`, `rate`, and `search`
3. turns that traffic into replay fixtures
4. creates a sandbox namespace where real `search` talks to dummy `geo` and `rate`
5. load-tests sandboxed `search`
6. writes result and model artifacts under `sandboxing/output/search`

## Prerequisites

Install these before starting:

- Docker
- Minikube
- `kubectl`
- Python 3.11+ with `venv`

You also need a working Minikube cluster:

```bash
minikube start
```

## One-time setup on a new system

### 1. Create the Python virtual environment

```bash
python3 -m venv hotelReservation/sandboxing/.venv
. hotelReservation/sandboxing/.venv/bin/activate
pip install --upgrade pip
pip install -r hotelReservation/sandboxing/requirements.txt
```

### 2. Build and load the images used by the experiment

Run this from the repo root:

```bash
bash hotelReservation/sandboxing/build_images.sh
```

This builds and loads:

- `deathstarbench/hotel-reservation:mvp`
- `deathstarbench/hotel-reservation:latest`
- `deathstarbench/hotel-reservation-sandbox-dummy:mvp`
- `deathstarbench/hotel-reservation-sandbox-dummy:latest`

## Deploy the baseline Hotel Reservation app

Apply the baseline Kubernetes manifests:

```bash
kubectl apply -Rf hotelReservation/kubernetes/
```

Wait until the baseline pods are running:

```bash
kubectl get pods
```

Make sure the baseline `geo`, `rate`, and `search` deployments use the locally
built image:

```bash
kubectl set image deployment/geo hotel-reserv-geo=deathstarbench/hotel-reservation:mvp
kubectl set image deployment/rate hotel-reserv-rate=deathstarbench/hotel-reservation:mvp
kubectl set image deployment/search hotel-reserv-search=deathstarbench/hotel-reservation:mvp

kubectl rollout status deployment/geo
kubectl rollout status deployment/rate
kubectl rollout status deployment/search
```

## Start the frontend port-forward

The capture step sends warmup HTTP requests to `http://127.0.0.1:8080`, so the
frontend port-forward must be running in a separate terminal:

```bash
kubectl port-forward svc/frontend 8080:5000
```

Leave that terminal open while the experiment runs.

## Run the experiment

From another terminal:

```bash
cd /path/to/DeathStarBench
. hotelReservation/sandboxing/.venv/bin/activate

python -m hotelReservation.sandboxing.experiment \
  hotelReservation/sandboxing/examples/search-experiment.json
```

## Expected outputs

After a successful run, look here:

- `sandboxing/output/search/capture/`
- `sandboxing/output/search/fixtures/`
- `sandboxing/output/search/results/search-run.json`
- `sandboxing/output/search/model/dataset.csv`
- `sandboxing/output/search/model/msc-model.pkl`
- `sandboxing/output/search/sandbox/search-sandbox.yaml`

## Fast reruns

### Reuse existing captured fixtures

If capture artifacts already exist and you only want to rerun sandbox apply and
measurement:

```bash
python -m hotelReservation.sandboxing.experiment \
  hotelReservation/sandboxing/examples/search-experiment.json \
  --skip-capture
```

### Reuse capture and skip sandbox apply

If the sandbox is already deployed and you only want to rerun measurement:

```bash
python -m hotelReservation.sandboxing.experiment \
  hotelReservation/sandboxing/examples/search-experiment.json \
  --skip-capture \
  --skip-apply
```

## When to rebuild images

Run this again whenever you change:

- `hotelReservation/Dockerfile`
- Go services under `hotelReservation/cmd/`, `services/`, or `sandbox/`
- dummy server code under `hotelReservation/sandboxing/dummy_server.py`
- sandboxing Python code that is baked into the dummy image

```bash
bash hotelReservation/sandboxing/build_images.sh
```

If you changed the app image, refresh the baseline deployments again:

```bash
kubectl set image deployment/geo hotel-reserv-geo=deathstarbench/hotel-reservation:mvp
kubectl set image deployment/rate hotel-reserv-rate=deathstarbench/hotel-reservation:mvp
kubectl set image deployment/search hotel-reserv-search=deathstarbench/hotel-reservation:mvp

kubectl rollout status deployment/geo
kubectl rollout status deployment/rate
kubectl rollout status deployment/search
```

## Useful checks

### Run the sandboxing unit tests

```bash
. hotelReservation/sandboxing/.venv/bin/activate
python -m unittest discover -s hotelReservation/sandboxing/tests
```

### Check sandbox pods

```bash
kubectl get pods -n hotelres-search-sandbox
```

### Check sandbox search logs

```bash
kubectl logs -n hotelres-search-sandbox deployment/search --tail=50
```

### Check baseline capture files inside pods

```bash
kubectl exec "$(kubectl get pods -l io.kompose.service=geo -o jsonpath='{.items[0].metadata.name}')" -- wc -l /tmp/geo.ndjson
kubectl exec "$(kubectl get pods -l io.kompose.service=rate -o jsonpath='{.items[0].metadata.name}')" -- wc -l /tmp/rate.ndjson
kubectl exec "$(kubectl get pods -l io.kompose.service=search -o jsonpath='{.items[0].metadata.name}')" -- wc -l /tmp/search.ndjson
```

## Common failure modes

### `No capture data found at /tmp/geo.ndjson`

Usually means one of these:

- the baseline pods are not using the rebuilt local image
- the frontend port-forward is not running on `127.0.0.1:8080`
- baseline `geo`, `rate`, or `search` did not roll out cleanly

Fix:

1. rerun `bash hotelReservation/sandboxing/build_images.sh`
2. refresh baseline images with `kubectl set image ...`
3. restart the frontend port-forward
4. rerun the experiment

### `metrics not available yet`

This usually resolves on its own after a short delay if Minikube metrics-server
is healthy. The current experiment already retries this case.

### `msc_rps` is low

That now means the experiment actually ran and measured a real limit for the
given config. Start by increasing:

- `cpu_millicores`
- `memory_mb`
- `replicas`

in [search-experiment.json](./examples/search-experiment.json), then rerun.
