from datetime import datetime, date
import pandas as pd
import pendulum
from decouple import config
from pytz import UTC
from sqlalchemy import create_engine, text


from src.workers.gold.load_gold_data import load_gold_daily_data_to_postgres_worker
from src.workers.gold.transform_silver_data import get_df_data

# --- DB connection ---
engine = create_engine(config("DB_CONN_RAW"))

# --- Force "today" to match test data ---
today = pendulum.today("UTC").date()

# --- Silver-like raw input (multiple rows per place) ---
silver_df = pd.DataFrame([
    {
        "place_name": "Testville",
        "forecast_date_utc": today,
        "forecast_hour_utc": 10,
        "ingest_date": today,
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
    {
        "place_name": "Testville",
        "forecast_date_utc": today,
        "forecast_hour_utc": 11,
        "ingest_date": today,
        "ingest_hour": 8,
        "temp_max": 12,
        "temp_min": 1,
        "temp_avg": 7,
        "rain": 1,
        "snow": 0,
        "wind_speed": 4,
        "clouds": 40,
        "humidity": 60,
    },
    # This one should be filtered OUT
    {
        "place_name": "OldPlace",
        "forecast_date_utc": today.replace(day=today.day - 1),
        "forecast_hour_utc": 10,
        "ingest_date": today,
        "ingest_hour": 8,
        "temp_max": 5,
        "temp_min": 0,
        "temp_avg": 2,
        "rain": 0,
        "snow": 0,
        "wind_speed": 2,
        "clouds": 10,
        "humidity": 30,
    },
])

silver_results = [(pd.Timestamp.now(), silver_df)]

# --- Run transform (this applies filter + aggregation) ---
generated_at = pendulum.now("UTC")
gold_results = get_df_data(silver_results, generated_at)

# --- Load to DB ---
load_gold_daily_data_to_postgres_worker(gold_results, engine)

# --- Validate results ---
with engine.connect() as conn:
    rows = conn.execute(
        text("SELECT place_name, forecast_date_utc FROM gold_daily_forecast_data WHERE place_name LIKE 'Test%'")
    ).fetchall()

print("Inserted rows:", rows)