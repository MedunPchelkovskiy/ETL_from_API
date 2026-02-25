import pandas as pd
import pendulum
from decouple import config
from sqlalchemy import create_engine, text

from src.workers.gold.load_gold_data import load_five_day_data_to_postgres_worker  # adjust if needed
from src.workers.gold.transform_silver_data import get_fdf_data  # adjust if needed

# --- DB connection ---
engine = create_engine(config("DB_CONN_RAW"))

# --- Base date (snapshot ts date) ---
base_date = pendulum.today("UTC").date()

# --- Create 7 days of forecast data ---
# Only first 5 (today + 4) should be kept
silver_df = pd.DataFrame([
    # Day 0 (today)
    {
        "place_name": "Testville",
        "forecast_date_utc": base_date,
        "forecast_hour_utc": 10,
        "ingest_date": base_date,
        "ingest_hour": 8,
        "temp_max": 10,
        "temp_min": 2,
        "temp_avg": 6,
        "rain": 0,
        "snow": 0,
        "wind_speed": 3,
        "clouds": 20,
        "humidity": 50,
    },
    # Day 1
    {
        "place_name": "Testville",
        "forecast_date_utc": base_date.add(days=1),
        "forecast_hour_utc": 10,
        "ingest_date": base_date,
        "ingest_hour": 8,
        "temp_max": 11,
        "temp_min": 3,
        "temp_avg": 7,
        "rain": 0,
        "snow": 0,
        "wind_speed": 4,
        "clouds": 30,
        "humidity": 55,
    },
    # Day 2
    {
        "place_name": "Testville",
        "forecast_date_utc": base_date.add(days=2),
        "forecast_hour_utc": 10,
        "ingest_date": base_date,
        "ingest_hour": 8,
        "temp_max": 12,
        "temp_min": 4,
        "temp_avg": 8,
        "rain": 0,
        "snow": 0,
        "wind_speed": 5,
        "clouds": 40,
        "humidity": 60,
    },
    # Day 3
    {
        "place_name": "Testville",
        "forecast_date_utc": base_date.add(days=3),
        "forecast_hour_utc": 10,
        "ingest_date": base_date,
        "ingest_hour": 8,
        "temp_max": 13,
        "temp_min": 5,
        "temp_avg": 9,
        "rain": 0,
        "snow": 0,
        "wind_speed": 6,
        "clouds": 50,
        "humidity": 65,
    },
    # Day 4
    {
        "place_name": "Testville",
        "forecast_date_utc": base_date.add(days=4),
        "forecast_hour_utc": 10,
        "ingest_date": base_date,
        "ingest_hour": 8,
        "temp_max": 14,
        "temp_min": 6,
        "temp_avg": 10,
        "rain": 0,
        "snow": 0,
        "wind_speed": 7,
        "clouds": 60,
        "humidity": 70,
    },
    # Day 6 (should be filtered OUT)
    {
        "place_name": "Testville",
        "forecast_date_utc": base_date.add(days=6),
        "forecast_hour_utc": 10,
        "ingest_date": base_date,
        "ingest_hour": 8,
        "temp_max": 20,
        "temp_min": 10,
        "temp_avg": 15,
        "rain": 0,
        "snow": 0,
        "wind_speed": 8,
        "clouds": 70,
        "humidity": 80,
    },
])

# Snapshot timestamp (important for snapshot-based logic)
ts = pendulum.now("UTC")

silver_results = [(ts, silver_df)]

# --- Run transform (should filter between ts.date() and ts.date()+4) ---
generated_at = pendulum.now("UTC")
gold_results = get_fdf_data(silver_results, generated_at)

# --- Load to DB ---
load_five_day_data_to_postgres_worker(gold_results, engine)

# --- Validate results ---
with engine.connect() as conn:
    rows = conn.execute(
        text("""
            SELECT place_name, forecast_date_utc 
            FROM gold_five_day_forecast_data 
            WHERE place_name = 'Testville'
            ORDER BY forecast_date_utc
        """)
    ).fetchall()

print("Inserted rows:", rows)
