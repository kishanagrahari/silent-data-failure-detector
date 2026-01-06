import pandas as pd
from pathlib import Path

RAW_BASE = Path("data/raw/weather")
METRICS_PATH = Path("data/metrics/weather_metrics.parquet")


def load_raw_data():
    # Find all date partitions like date=YYYY-MM-DD
    partitions = sorted(RAW_BASE.glob("date=*"))

    if not partitions:
        raise FileNotFoundError("No raw data partitions found")

    # Pick the latest available partition
    latest_partition = partitions[-1]
    data_path = latest_partition / "weather.parquet"

    print(f"Loading raw data from {latest_partition.name}")
    return pd.read_parquet(data_path), latest_partition.name.replace("date=", "")


def compute_metrics(df, run_date):
    metrics = {
        "run_date": run_date,
        "row_count": len(df),
        "null_temperature_pct": df["temperature"].isna().mean(),
        "null_humidity_pct": df["humidity"].isna().mean(),
        "null_wind_speed_pct": df["wind_speed"].isna().mean(),
        "mean_temperature": df["temperature"].mean(),
        "mean_humidity": df["humidity"].mean(),
        "mean_wind_speed": df["wind_speed"].mean(),
        "distinct_weather_codes": df["weather_code"].nunique()
    }
    return pd.DataFrame([metrics])


def save_metrics(metrics_df):
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)

    if METRICS_PATH.exists():
        existing = pd.read_parquet(METRICS_PATH)
        combined = pd.concat([existing, metrics_df], ignore_index=True)
    else:
        combined = metrics_df

    combined.to_parquet(METRICS_PATH, index=False)
    print(f"Saved metrics to {METRICS_PATH}")


if __name__ == "__main__":
    df, run_date = load_raw_data()
    metrics_df = compute_metrics(df, run_date)
    save_metrics(metrics_df)
