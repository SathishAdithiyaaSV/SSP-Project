#!/usr/bin/env python3
"""Generate MSC charts from versioned measurement snapshots.

Charts go to ``artifacts/analysis/performance-plots`` by default. Use
``--output artifacts/report-figures`` to regenerate the report charts.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
MSC_COLUMNS = ("msc", "msc_rps", "max_rps", "max_sustainable_rps", "observed_msc")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--main", type=Path, default=REPO_ROOT / "data/measurements/dataset_others.csv")
    parser.add_argument("--search", type=Path, default=REPO_ROOT / "data/measurements/dataset_search.csv")
    parser.add_argument("--output", type=Path, default=REPO_ROOT / "artifacts/analysis/performance-plots")
    return parser.parse_args()


def msc_column(frame: pd.DataFrame) -> str:
    for column in MSC_COLUMNS:
        if column in frame.columns:
            return column
    raise ValueError(f"No MSC column found; expected {MSC_COLUMNS}, got {list(frame.columns)}")


def save(output: Path, filename: str) -> None:
    plt.tight_layout()
    plt.savefig(output / filename, dpi=160)
    plt.close()


def plot_msc_vs_cpu(frame: pd.DataFrame, msc: str, service: str, output: Path) -> None:
    plt.figure(figsize=(8, 5))
    for replicas in sorted(frame.replicas.unique()):
        subset = frame[frame.replicas == replicas].sort_values("cpu_millicores")
        plt.plot(subset.cpu_millicores, subset[msc], marker="o", label=f"{replicas} replicas")
    plt.xlabel("CPU (millicores)")
    plt.ylabel("MSC / max sustainable RPS")
    plt.title(f"{service}: MSC vs CPU")
    plt.legend()
    plt.grid(True, alpha=0.3)
    save(output, f"{service}_msc_vs_cpu.png")


def plot_msc_vs_replicas(frame: pd.DataFrame, msc: str, service: str, output: Path) -> None:
    plt.figure(figsize=(8, 5))
    for cpu in sorted(frame.cpu_millicores.unique()):
        subset = frame[frame.cpu_millicores == cpu].sort_values("replicas")
        plt.plot(subset.replicas, subset[msc], marker="o", label=f"{cpu}m CPU")
    plt.xlabel("Replicas")
    plt.ylabel("MSC / max sustainable RPS")
    plt.title(f"{service}: MSC vs replicas")
    plt.legend()
    plt.grid(True, alpha=0.3)
    save(output, f"{service}_msc_vs_replicas.png")


def plot_efficiency(frame: pd.DataFrame, msc: str, service: str, output: Path) -> None:
    frame = frame.copy()
    frame["total_cpu"] = frame.cpu_millicores * frame.replicas
    frame["cpu_efficiency"] = frame[msc] / frame.total_cpu
    plt.figure(figsize=(8, 5))
    plt.plot(frame.total_cpu, frame.cpu_efficiency, marker="o")
    plt.xlabel("Total CPU allocated (millicores)")
    plt.ylabel("MSC per millicore")
    plt.title(f"{service}: CPU efficiency")
    plt.grid(True, alpha=0.3)
    save(output, f"{service}_cpu_efficiency.png")


def main() -> None:
    args = parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    main_data, search_data = pd.read_csv(args.main), pd.read_csv(args.search)
    for frame in (main_data, search_data):
        frame.columns = [column.strip().lower() for column in frame.columns]
    if "service" not in search_data:
        search_data["service"] = "search"

    main_msc, search_msc = msc_column(main_data), msc_column(search_data)
    combined = pd.concat((main_data, search_data), ignore_index=True, sort=False)
    for service in combined.service.dropna().unique():
        service_data = combined[combined.service == service].copy()
        service_msc = search_msc if service == "search" else main_msc
        plot_msc_vs_cpu(service_data, service_msc, service, args.output)
        plot_msc_vs_replicas(service_data, service_msc, service, args.output)
        plot_efficiency(service_data, service_msc, service, args.output)

    grouped = combined.groupby("service")[main_msc].mean().sort_values()
    plt.figure(figsize=(10, 6))
    grouped.plot(kind="bar")
    plt.ylabel("Average MSC")
    plt.title("Average MSC by microservice")
    plt.grid(axis="y", alpha=0.3)
    save(args.output, "service_comparison_avg_msc.png")

    summary = combined.copy()
    summary["total_cpu"] = summary.cpu_millicores * summary.replicas
    summary["cpu_efficiency"] = summary[main_msc] / summary.total_cpu
    summary.to_csv(args.output / "performance_summary.csv", index=False)
    print(f"Charts and summary written to {args.output.resolve()}")


if __name__ == "__main__":
    main()
