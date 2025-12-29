import requests
import pandas as pd
from pathlib import Path
from datetime import date

# -------- CONFIG --------
LATITUDE = 40.7128      # New York City
LONGITUDE = -74.0060
OUTPUT_BASE = Path("data/raw/weather")
TODAY = date.today().isoformat()

# ------------------------

def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m",
            "weather_code"
        ]
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def transform(raw):
    df = pd.DataFrame({
        "time": raw["hourly"]["time"],
        "temperature": raw["hourly"]["temperature_2m"],
        "humidity": raw["hourly"]["relative_humidity_2m"],
        "wind_speed": raw["hourly"]["wind_speed_10m"],
        "weather_code": raw["hourly"]["weather_code"]
    })

    df["date"] = pd.to_datetime(df["time"]).dt.date
    df["city"] = "New York"
    return df


def save_raw(df):
    output_dir = OUTPUT_BASE / f"date={TODAY}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "weather.parquet"
    df.to_parquet(output_file, index=False)
    print(f"Saved raw data to {output_file}")


if __name__ == "__main__":
    raw = fetch_weather()
    df = transform(raw)
    save_raw(df)

