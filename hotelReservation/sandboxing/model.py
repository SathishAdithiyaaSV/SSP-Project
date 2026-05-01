from __future__ import annotations

import csv
import json
import pickle
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


# Features known at prediction time (not observed during experiment)
FEATURES = ["cpu_millicores", "replicas"]


# ---------------------------------------------------------------------------
# Model classes
# ---------------------------------------------------------------------------

class ConstantMSCModel:
    """Fallback when only one data point is available."""
    def __init__(self, value: float):
        self.value = value

    def predict(self, rows: List[List[float]]) -> List[float]:
        return [self.value for _ in rows]

    def __repr__(self) -> str:
        return f"ConstantMSCModel(value={self.value:.1f})"


class LinearMSCModel:
    """Linear regression: MSC = intercept + a*cpu + b*replicas"""
    def __init__(self, coefficients: List[float], intercept: float, feature_names: List[str]):
        self.coefficients = coefficients
        self.intercept = intercept
        self.feature_names = feature_names

    def predict(self, rows: List[List[float]]) -> List[float]:
        preds = []
        for row in rows:
            value = self.intercept
            for coef, feature in zip(self.coefficients, row):
                value += coef * feature
            preds.append(max(0.0, value))  # MSC cannot be negative
        return preds

    def __repr__(self) -> str:
        terms = " + ".join(
            f"{c:.4f}*{n}" for c, n in zip(self.coefficients, self.feature_names)
        )
        return f"LinearMSCModel(MSC = {self.intercept:.2f} + {terms})"


# ---------------------------------------------------------------------------
# Per-service model collection
# ---------------------------------------------------------------------------

class MSCModelCollection:
    """
    Holds one model per service. Used to predict MSC given
    (service, cpu_millicores, replicas).
    """
    def __init__(self, models: Dict[str, LinearMSCModel | ConstantMSCModel]):
        self.models = models

    def predict(self, service: str, cpu_millicores: int, replicas: int) -> Optional[float]:
        """
        Predict MSC for a given service configuration.

        Args:
            service: service name e.g. "geo", "profile"
            cpu_millicores: CPU limit per pod in millicores e.g. 400
            replicas: number of pod replicas e.g. 2

        Returns:
            Predicted MSC in RPS, or None if service not in model.
        """
        model = self.models.get(service)
        if model is None:
            return None
        row = [float(cpu_millicores), float(replicas)]
        return model.predict([row])[0]

    def services(self) -> List[str]:
        return sorted(self.models.keys())

    def summary(self) -> str:
        lines = ["MSC Model Summary", "=" * 50]
        for service, model in sorted(self.models.items()):
            lines.append(f"  {service}: {model}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def fit_model(rows: Iterable[dict], output_dir: Path) -> Path:
    """
    Fit one linear model per service on the provided rows.
    Filters out zero MSC rows (measurement artifacts) and censored
    max_rps_reached rows before fitting.

    Args:
        rows: iterable of dicts with keys:
              service, cpu_millicores, replicas, msc_rps, violation_reason
        output_dir: directory to write msc-model.pkl and model-summary.json

    Returns:
        Path to the saved model pickle.
    """
    rows = list(rows)
    if not rows:
        raise ValueError("no experiment rows to fit")

    # Filter out zero MSC rows (flush overhead artifact) and censored rows
    clean_rows = [
        r for r in rows
        if float(r["msc_rps"]) > 0
        and r.get("violation_reason", "") != "max_rps_reached"
    ]

    censored_rows = [
        r for r in rows
        if r.get("violation_reason", "") == "max_rps_reached"
    ]

    print(f"Total rows: {len(rows)}")
    print(f"Zero MSC rows dropped: {len([r for r in rows if float(r['msc_rps']) == 0])}")
    print(f"Censored rows (max_rps_reached) excluded from fit: {len(censored_rows)}")
    print(f"Clean rows used for fitting: {len(clean_rows)}")

    # Group by service
    by_service: Dict[str, List[dict]] = {}
    for row in clean_rows:
        by_service.setdefault(row["service"], []).append(row)

    models: Dict[str, LinearMSCModel | ConstantMSCModel] = {}
    summary = {}

    for service, service_rows in sorted(by_service.items()):
        matrix = [[float(r[f]) for f in FEATURES] for r in service_rows]
        targets = [float(r["msc_rps"]) for r in service_rows]

        if len(service_rows) < 2:
            model = ConstantMSCModel(targets[0])
        else:
            model = _fit_linear_model(matrix, targets, FEATURES)

        models[service] = model

        # Compute R² on training data
        preds = model.predict(matrix)
        r2 = _r_squared(targets, preds)
        mae = _mean_absolute_error(targets, preds)

        print(f"  {service}: {model}")
        print(f"    R²={r2:.3f}  MAE={mae:.1f} RPS  n={len(service_rows)}")

        summary[service] = {
            "model": repr(model),
            "n_rows": len(service_rows),
            "r_squared": round(r2, 4),
            "mae_rps": round(mae, 2),
            "censored_rows_excluded": len([
                r for r in censored_rows if r["service"] == service
            ]),
        }

    collection = MSCModelCollection(models)

    # Save model
    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / "msc-model.pkl"
    with model_path.open("wb") as handle:
        pickle.dump({
            "model_collection": collection,
            "features": FEATURES,
        }, handle)

    # Save human-readable summary
    summary_path = output_dir / "model-summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))

    print(f"\nModel saved to {model_path}")
    print(f"Summary saved to {summary_path}")
    return model_path


def load_model(model_path: Path) -> MSCModelCollection:
    """Load a saved MSCModelCollection from disk."""
    with model_path.open("rb") as handle:
        payload = pickle.load(handle)
    return payload["model_collection"]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def cross_validate(rows: Iterable[dict], k: int = 3) -> Dict[str, dict]:
    """
    K-fold cross validation per service.
    Returns per-service validation metrics.
    """
    rows = list(rows)
    clean_rows = [
        r for r in rows
        if float(r["msc_rps"]) > 0
        and r.get("violation_reason", "") != "max_rps_reached"
    ]

    by_service: Dict[str, List[dict]] = {}
    for row in clean_rows:
        by_service.setdefault(row["service"], []).append(row)

    results = {}
    for service, service_rows in sorted(by_service.items()):
        n = len(service_rows)
        if n < k:
            results[service] = {
                "note": f"too few rows ({n}) for {k}-fold CV",
                "n_rows": n,
            }
            continue

        fold_size = n // k
        all_errors = []

        for fold in range(k):
            val_start = fold * fold_size
            val_end = val_start + fold_size if fold < k - 1 else n
            val_rows = service_rows[val_start:val_end]
            train_rows = service_rows[:val_start] + service_rows[val_end:]

            if len(train_rows) < 2:
                continue

            matrix = [[float(r[f]) for f in FEATURES] for r in train_rows]
            targets = [float(r["msc_rps"]) for r in train_rows]
            model = _fit_linear_model(matrix, targets, FEATURES)

            val_matrix = [[float(r[f]) for f in FEATURES] for r in val_rows]
            val_targets = [float(r["msc_rps"]) for r in val_rows]
            preds = model.predict(val_matrix)

            for actual, pred in zip(val_targets, preds):
                all_errors.append(abs(actual - pred))

        if all_errors:
            mae = sum(all_errors) / len(all_errors)
            results[service] = {
                "n_rows": n,
                "cv_mae_rps": round(mae, 2),
                "cv_folds": k,
            }
            print(f"  {service}: CV MAE = {mae:.1f} RPS (n={n}, k={k})")

    return results


# ---------------------------------------------------------------------------
# Prediction helpers
# ---------------------------------------------------------------------------

def recommend_config(
    model_collection: MSCModelCollection,
    service: str,
    target_rps: float,
    cpu_options: List[int] = None,
    replica_options: List[int] = None,
) -> List[dict]:
    """
    Given a target RPS, find all (cpu, replicas) configs that can handle it,
    sorted by total CPU cost (cpu_millicores * replicas).

    Args:
        model_collection: trained MSCModelCollection
        service: service name
        target_rps: required RPS to handle
        cpu_options: list of CPU configs to consider (millicores)
        replica_options: list of replica counts to consider

    Returns:
        List of viable configs sorted by cost, each with predicted MSC.
    """
    if cpu_options is None:
        cpu_options = [100, 200, 400, 800, 1000]
    if replica_options is None:
        replica_options = [1, 2, 3, 4]

    viable = []
    for cpu in cpu_options:
        for replicas in replica_options:
            predicted = model_collection.predict(service, cpu, replicas)
            if predicted is not None and predicted >= target_rps:
                viable.append({
                    "service": service,
                    "cpu_millicores": cpu,
                    "replicas": replicas,
                    "predicted_msc_rps": round(predicted, 1),
                    "total_cpu_millicores": cpu * replicas,
                    "headroom_rps": round(predicted - target_rps, 1),
                })

    viable.sort(key=lambda x: x["total_cpu_millicores"])
    return viable


# ---------------------------------------------------------------------------
# Linear algebra (no external deps)
# ---------------------------------------------------------------------------

def _fit_linear_model(
    matrix: List[List[float]],
    targets: List[float],
    feature_names: List[str],
) -> LinearMSCModel:
    design = [[1.0, *row] for row in matrix]
    xtx = _matmul(_transpose(design), design)
    xty = _matvec(_transpose(design), targets)

    # Ridge regularization for stability on small datasets
    ridge = 1e-4
    for i in range(len(xtx)):
        xtx[i][i] += ridge

    solution = _solve_linear_system(xtx, xty)
    intercept = solution[0]
    coefficients = solution[1:]
    return LinearMSCModel(
        coefficients=coefficients,
        intercept=intercept,
        feature_names=feature_names,
    )


def _r_squared(actual: List[float], predicted: List[float]) -> float:
    mean = sum(actual) / len(actual)
    ss_tot = sum((a - mean) ** 2 for a in actual)
    ss_res = sum((a - p) ** 2 for a, p in zip(actual, predicted))
    if ss_tot == 0:
        return 1.0
    return 1.0 - ss_res / ss_tot


def _mean_absolute_error(actual: List[float], predicted: List[float]) -> float:
    return sum(abs(a - p) for a, p in zip(actual, predicted)) / len(actual)


def _transpose(matrix: List[List[float]]) -> List[List[float]]:
    return [list(col) for col in zip(*matrix)]


def _matmul(left: List[List[float]], right: List[List[float]]) -> List[List[float]]:
    out = []
    for row in left:
        out_row = []
        for col in zip(*right):
            out_row.append(sum(a * b for a, b in zip(row, col)))
        out.append(out_row)
    return out


def _matvec(matrix: List[List[float]], vector: List[float]) -> List[float]:
    return [sum(a * b for a, b in zip(row, vector)) for row in matrix]


def _solve_linear_system(
    matrix: List[List[float]], vector: List[float]
) -> List[float]:
    size = len(vector)
    augmented = [row[:] + [vector[i]] for i, row in enumerate(matrix)]

    for col in range(size):
        pivot = max(range(col, size), key=lambda row: abs(augmented[row][col]))
        augmented[col], augmented[pivot] = augmented[pivot], augmented[col]
        pivot_val = augmented[col][col]
        if abs(pivot_val) < 1e-12:
            raise ValueError("singular matrix while fitting MSC model")
        for j in range(col, size + 1):
            augmented[col][j] /= pivot_val
        for row in range(size):
            if row == col:
                continue
            factor = augmented[row][col]
            for j in range(col, size + 1):
                augmented[row][j] -= factor * augmented[col][j]

    return [augmented[i][size] for i in range(size)]


# ---------------------------------------------------------------------------
# CLI — run directly to train and validate
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Train and validate MSC regression model.")
    parser.add_argument("--dataset", required=True, help="Path to dataset.csv")
    parser.add_argument("--output-dir", required=True, help="Directory to save model")
    parser.add_argument("--predict", nargs=3, metavar=("SERVICE", "CPU", "REPLICAS"),
                        help="After training, predict MSC for this config")
    parser.add_argument("--recommend", nargs=2, metavar=("SERVICE", "TARGET_RPS"),
                        help="Find cheapest config to handle TARGET_RPS for SERVICE")
    args = parser.parse_args()

    dataset_path = Path(args.dataset)
    output_dir = Path(args.output_dir)

    rows = list(csv.DictReader(dataset_path.open()))
    print(f"\n--- Training ---")
    model_path = fit_model(rows, output_dir)

    print(f"\n--- Cross Validation ---")
    collection = load_model(model_path)
    cv_results = cross_validate(rows)

    cv_path = output_dir / "cv-results.json"
    cv_path.write_text(json.dumps(cv_results, indent=2))
    print(f"CV results saved to {cv_path}")

    print(f"\n--- Model Summary ---")
    print(collection.summary())

    if args.predict:
        service, cpu, replicas = args.predict
        predicted = collection.predict(service, int(cpu), int(replicas))
        print(f"\n--- Prediction ---")
        print(f"{service} @ {cpu}m CPU, {replicas} replicas → predicted MSC = {predicted:.1f} RPS")

    if args.recommend:
        service, target_rps = args.recommend
        print(f"\n--- Recommendations for {service} @ {target_rps} RPS ---")
        configs = recommend_config(collection, service, float(target_rps))
        if not configs:
            print("  No viable config found within the trained range.")
        for cfg in configs:
            print(f"  cpu={cfg['cpu_millicores']}m replicas={cfg['replicas']} "
                  f"→ predicted={cfg['predicted_msc_rps']} RPS "
                  f"(headroom={cfg['headroom_rps']} RPS, "
                  f"total_cpu={cfg['total_cpu_millicores']}m)")