import pandas as pd
from pathlib import Path
import numpy as np

METRICS_PATH = Path("data/metrics/weather_metrics.parquet")
BASELINE_PATH = Path("data/baseline/weather_baseline.parquet")
ANOMALY_PATH = Path("data/anomalies/weather_anomalies.parquet")

Z_THRESHOLD = 3


def detect():
    if not BASELINE_PATH.exists():
        print("Baseline not found. Skipping anomaly detection.")
        return

    metrics = pd.read_parquet(METRICS_PATH)
    baseline = pd.read_parquet(BASELINE_PATH)

    latest = metrics.iloc[-1]
    run_date = latest["run_date"]

    current_baseline = baseline[baseline["run_date"] == run_date]

    if current_baseline.empty:
        print("No baseline available for latest run. Skipping detection.")
        return

    anomalies = []

    for _, row in current_baseline.iterrows():
        metric = row["metric"]
        mean = row["mean"]
        std = row["std"]
        value = latest[metric]

        if pd.isna(std) or std == 0:
            if value != mean:
                anomalies.append(
                    (run_date, metric, value, "freeze_or_constant")
                )
            continue

        z = (value - mean) / std

        if abs(z) > Z_THRESHOLD:
            anomalies.append(
                (run_date, metric, value, f"z_score_{round(z,2)}")
            )

    if anomalies:
        out = pd.DataFrame(
            anomalies,
            columns=["run_date", "metric", "value", "reason"]
        )
        ANOMALY_PATH.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(ANOMALY_PATH, index=False)
        print("Anomalies detected and saved.")
    else:
        print("No anomalies detected.")


if __name__ == "__main__":
    detect()
