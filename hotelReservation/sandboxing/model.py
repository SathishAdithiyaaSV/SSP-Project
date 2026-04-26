from __future__ import annotations

import csv
import pickle
from pathlib import Path
from typing import Iterable, List


FEATURES = ["cpu_millicores", "memory_mb", "replicas", "observed_cpu_millicores"]


class ConstantMSCModel:
    def __init__(self, value: float):
        self.value = value

    def predict(self, rows: List[List[float]]) -> List[float]:
        return [self.value for _ in rows]


class LinearMSCModel:
    def __init__(self, coefficients: List[float], intercept: float):
        self.coefficients = coefficients
        self.intercept = intercept

    def predict(self, rows: List[List[float]]) -> List[float]:
        preds = []
        for row in rows:
            value = self.intercept
            for coef, feature in zip(self.coefficients, row):
                value += coef * feature
            preds.append(value)
        return preds


def fit_model(rows: Iterable[dict], output_dir: Path) -> Path:
    rows = list(rows)
    if not rows:
        raise ValueError("no experiment rows to fit")

    if len(rows) < 2:
        model = ConstantMSCModel(float(rows[0]["msc_rps"]))
    else:
        matrix = [[float(row[name]) for name in FEATURES] for row in rows]
        targets = [float(row["msc_rps"]) for row in rows]
        model = _fit_linear_model(matrix, targets)

    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / "msc-model.pkl"
    with model_path.open("wb") as handle:
        pickle.dump({"model": model, "features": FEATURES}, handle)

    with (output_dir / "dataset.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["service", *FEATURES, "msc_rps"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return model_path


def _fit_linear_model(matrix: List[List[float]], targets: List[float]) -> LinearMSCModel:
    design = [[1.0, *row] for row in matrix]
    xtx = _matmul(_transpose(design), design)
    xty = _matvec(_transpose(design), targets)

    # Small ridge term keeps the normal equation stable for tiny datasets.
    for index in range(len(xtx)):
        xtx[index][index] += 1e-6

    solution = _solve_linear_system(xtx, xty)
    intercept = solution[0]
    coefficients = solution[1:]
    return LinearMSCModel(coefficients=coefficients, intercept=intercept)


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


def _solve_linear_system(matrix: List[List[float]], vector: List[float]) -> List[float]:
    size = len(vector)
    augmented = [row[:] + [vector[index]] for index, row in enumerate(matrix)]

    for col in range(size):
        pivot = max(range(col, size), key=lambda row: abs(augmented[row][col]))
        augmented[col], augmented[pivot] = augmented[pivot], augmented[col]
        pivot_value = augmented[col][col]
        if abs(pivot_value) < 1e-12:
            raise ValueError("singular matrix while fitting MSC model")

        for j in range(col, size + 1):
            augmented[col][j] /= pivot_value

        for row in range(size):
            if row == col:
                continue
            factor = augmented[row][col]
            for j in range(col, size + 1):
                augmented[row][j] -= factor * augmented[col][j]

    return [augmented[i][size] for i in range(size)]
