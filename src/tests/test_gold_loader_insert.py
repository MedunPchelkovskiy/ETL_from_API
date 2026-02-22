from datetime import datetime, date

import pandas as pd
from decouple import config
from pytz import UTC
from sqlalchemy import create_engine, text

from src.workers.gold.load_gold_data import load_gold_daily_data_to_postgres_worker

# --- Настройка на връзка към тестова база / локален Postgres ---
engine = create_engine(config("DB_CONN_RAW"))

# --- Тестови данни (само 2 реда, в памет) ---
test_df = pd.DataFrame([
    {
        "place_name": "Testville",
        "ingest_date": date(2026, 2, 21),
        "ingest_hour": 12,
        "forecast_date_utc": date(2026, 2, 22),
        "forecast_hour_utc": 12,
        "generated_at": datetime.now(UTC),  # timezone-aware
        "temp_max": 10.0,
        "temp_min": 2.0,
        "temp_avg": 6.0,
        "rain_min": 0.0,
        "rain_max": 1.0,
        "rain_avg": 0.5,
        "snow_min": 0.0,
        "snow_max": 0.0,
        "snow_avg": 0.0,
        "wind_speed_min": 1.0,
        "wind_speed_max": 5.0,
        "wind_speed_avg": 3.0,
        "cloud_cover_min": 0,
        "cloud_cover_max": 50,
        "cloud_cover_avg": 25,
        "humidity_min": 30,
        "humidity_max": 70,
        "humidity_avg": 50
    },
    {
        "place_name": "Testopolis",
        "ingest_date": date(2026, 2, 21),
        "ingest_hour": 12,
        "forecast_date_utc": date(2026, 2, 23),
        "forecast_hour_utc": 12,
        "generated_at": datetime.now(UTC),  # timezone-aware
        "temp_max": 12.0,
        "temp_min": 4.0,
        "temp_avg": 8.0,
        "rain_min": 0.0,
        "rain_max": 2.0,
        "rain_avg": 1.0,
        "snow_min": 0.0,
        "snow_max": 0.0,
        "snow_avg": 0.0,
        "wind_speed_min": 2.0,
        "wind_speed_max": 6.0,
        "wind_speed_avg": 4.0,
        "cloud_cover_min": 10,
        "cloud_cover_max": 60,
        "cloud_cover_avg": 35,
        "humidity_min": 40,
        "humidity_max": 80,
        "humidity_avg": 60
    }
])

# --- Подаваме го като list на loader-а ---
gold_results = [(pd.Timestamp.now(), test_df)]

# --- Извикване на loader-а ---
load_gold_daily_data_to_postgres_worker(gold_results, engine)

# --- Проверка на записаните редове ---
with engine.connect() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM gold_daily_forecast_data WHERE place_name LIKE 'Test%'")).scalar()
    print(f"Rows inserted for test places: {count}")