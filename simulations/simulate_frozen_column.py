import pandas as pd
from pathlib import Path

RAW_BASE = Path("data/raw/weather")

latest = sorted(RAW_BASE.glob("date=*"))[-1]
df = pd.read_parquet(latest / "weather.parquet")

df["temperature"] = df["temperature"].iloc[0]

df.to_parquet(latest / "weather.parquet", index=False)

print("Simulated frozen temperature column")

