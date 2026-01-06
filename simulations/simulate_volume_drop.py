import pandas as pd
from pathlib import Path

RAW_BASE = Path("data/raw/weather")

latest = sorted(RAW_BASE.glob("date=*"))[-1]
df = pd.read_parquet(latest / "weather.parquet")

# Drop ~50% of rows
df_dropped = df.sample(frac=0.5, random_state=42)

output = latest / "weather.parquet"
df_dropped.to_parquet(output, index=False)

print(f"Simulated volume drop. Rows: {len(df)} → {len(df_dropped)}")

