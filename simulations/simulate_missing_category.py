import pandas as pd
from pathlib import Path

RAW_BASE = Path("data/raw/weather")

latest = sorted(RAW_BASE.glob("date=*"))[-1]
df = pd.read_parquet(latest / "weather.parquet")

missing_code = df["weather_code"].unique()[0]
df = df[df["weather_code"] != missing_code]

df.to_parquet(latest / "weather.parquet", index=False)

print(f"Simulated missing category: weather_code={missing_code}")

