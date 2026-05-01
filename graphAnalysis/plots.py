import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ----------------------------
# CONFIG
# ----------------------------
DATASET_MAIN = "dataset_others.csv"          # multi-microservice results
DATASET_SEARCH = "dataset_search.csv"    # search-specific results
OUTPUT_DIR = Path("performance_plots")
OUTPUT_DIR.mkdir(exist_ok=True)

# ----------------------------
# LOAD DATA
# ----------------------------
df_main = pd.read_csv(DATASET_MAIN)
df_search = pd.read_csv(DATASET_SEARCH)

# Normalize column names
for df in [df_main, df_search]:
    df.columns = [c.strip().lower() for c in df.columns]

# Expected columns:
# service, cpu_millicores, replicas, msc(or max sustainable rps), success_rate, p90_latency_ms
# Adjust if needed:
MSC_COL_CANDIDATES = ["msc", "msc_rps", "max_rps", "max_sustainable_rps", "observed_msc"]


def find_msc_col(df):
    for col in MSC_COL_CANDIDATES:
        if col in df.columns:
            return col
    raise ValueError(f"No MSC-like column found in {df.columns}")


main_msc_col = find_msc_col(df_main)
search_msc_col = find_msc_col(df_search)

# Add service name if missing in search dataset
if "service" not in df_search.columns:
    df_search["service"] = "search"

# Merge for unified analysis
combined = pd.concat([
    df_main[[c for c in df_main.columns]],
    df_search[[c for c in df_search.columns if c in df_main.columns or c == "service"]]
], ignore_index=True, sort=False)

# ----------------------------
# HELPER FUNCTIONS
# ----------------------------
def save_plot(filename):
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / filename)
    plt.close()


def plot_msc_vs_cpu(df, msc_col, service_name):
    plt.figure(figsize=(8, 5))
    for replica in sorted(df["replicas"].unique()):
        subset = df[df["replicas"] == replica].sort_values("cpu_millicores")
        plt.plot(
            subset["cpu_millicores"],
            subset[msc_col],
            marker="o",
            label=f"{replica} replicas"
        )
    plt.xlabel("CPU (millicores)")
    plt.ylabel("MSC / Max Sustainable RPS")
    plt.title(f"{service_name}: MSC vs CPU")
    plt.legend()
    plt.grid(True)
    save_plot(f"{service_name}_msc_vs_cpu.png")


def plot_msc_vs_replicas(df, msc_col, service_name):
    plt.figure(figsize=(8, 5))
    for cpu in sorted(df["cpu_millicores"].unique()):
        subset = df[df["cpu_millicores"] == cpu].sort_values("replicas")
        plt.plot(
            subset["replicas"],
            subset[msc_col],
            marker="o",
            label=f"{cpu}m CPU"
        )
    plt.xlabel("Replicas")
    plt.ylabel("MSC / Max Sustainable RPS")
    plt.title(f"{service_name}: MSC vs Replicas")
    plt.legend()
    plt.grid(True)
    save_plot(f"{service_name}_msc_vs_replicas.png")


def plot_efficiency(df, msc_col, service_name):
    df = df.copy()
    df["total_cpu"] = df["cpu_millicores"] * df["replicas"]
    df["cpu_efficiency"] = df[msc_col] / df["total_cpu"]

    plt.figure(figsize=(8, 5))
    plt.plot(
        df["total_cpu"],
        df["cpu_efficiency"],
        marker="o",
        linestyle="-"
    )
    plt.xlabel("Total CPU Allocated (millicores)")
    plt.ylabel("MSC per millicore")
    plt.title(f"{service_name}: CPU Efficiency")
    plt.grid(True)
    save_plot(f"{service_name}_cpu_efficiency.png")


def plot_latency_curve(df, service_name):
    if "p90_latency_ms" not in df.columns:
        return
    plt.figure(figsize=(8, 5))
    for replica in sorted(df["replicas"].unique()):
        subset = df[df["replicas"] == replica].sort_values("cpu_millicores")
        plt.plot(
            subset["cpu_millicores"],
            subset["p90_latency_ms"],
            marker="o",
            label=f"{replica} replicas"
        )
    plt.xlabel("CPU (millicores)")
    plt.ylabel("P90 Latency (ms)")
    plt.title(f"{service_name}: P90 Latency vs CPU")
    plt.legend()
    plt.grid(True)
    save_plot(f"{service_name}_latency_vs_cpu.png")


def plot_service_comparison(df_all, msc_col):
    grouped = (
        df_all.groupby("service")[msc_col]
        .mean()
        .sort_values()
    )

    plt.figure(figsize=(10, 6))
    grouped.plot(kind="bar")
    plt.ylabel("Average MSC")
    plt.title("Average MSC by Microservice")
    plt.grid(axis="y")
    save_plot("service_comparison_avg_msc.png")


# ----------------------------
# MAIN ANALYSIS
# ----------------------------
services = combined["service"].dropna().unique()

for service in services:
    service_df = combined[combined["service"] == service].copy()
    msc_col = search_msc_col if service == "search" else main_msc_col

    if service_df.empty:
        continue

    plot_msc_vs_cpu(service_df, msc_col, service)
    plot_msc_vs_replicas(service_df, msc_col, service)
    plot_efficiency(service_df, msc_col, service)
    plot_latency_curve(service_df, service)

# Overall comparison
plot_service_comparison(combined, main_msc_col if main_msc_col in combined.columns else search_msc_col)

# ----------------------------
# SUMMARY TABLE
# ----------------------------
summary = combined.copy()
summary["total_cpu"] = summary["cpu_millicores"] * summary["replicas"]
msc_col_global = main_msc_col if main_msc_col in summary.columns else search_msc_col
summary["cpu_efficiency"] = summary[msc_col_global] / summary["total_cpu"]

summary.to_csv(OUTPUT_DIR / "performance_summary.csv", index=False)

print(f"Plots and summary saved to: {OUTPUT_DIR.resolve()}")
