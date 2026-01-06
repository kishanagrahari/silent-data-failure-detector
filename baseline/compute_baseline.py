import pandas as pd
from pathlib import Path

METRICS_PATH = Path("data/metrics/weather_metrics.parquet")
BASELINE_PATH = Path("data/baseline/weather_baseline.parquet")

WINDOW = 14  # rolling window size


def compute_baseline():
    df = pd.read_parquet(METRICS_PATH)

    if len(df) < 2:
        print("Not enough history to compute baseline")
        return

    numeric_cols = df.select_dtypes(include="number").columns

    baseline_records = []

    for col in numeric_cols:
        rolling = df[col].rolling(WINDOW)

        temp = pd.DataFrame({
            "run_date": df["run_date"],
            "metric": col,
            "mean": rolling.mean(),
            "std": rolling.std()
        })

        baseline_records.append(temp)

    baseline = pd.concat(baseline_records, ignore_index=True)

    BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    baseline.to_parquet(BASELINE_PATH, index=False)

    print(f"Saved baseline to {BASELINE_PATH}")


if __name__ == "__main__":
    compute_baseline()
