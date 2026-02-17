import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.helpers.logging_helpers.combine_loggers_helper import get_logger


def load_silver_data_to_postgres_worker(df: pd.DataFrame, engine):
    """
    Production-ready loader for silver_weather_forecast_data using custom sanitizer.

    Args:
        df (pd.DataFrame): Data to insert.
        engine: SQLAlchemy Engine connected to Postgres.
    """
    logger = get_logger()

    if df is None or df.empty:
        logger.warning("Silver load skipped: empty dataframe")
        return

    # Ensure required columns
    required_cols = [
        "api_name", "place_name", "ingest_date", "ingest_hour",
        "original_ts", "forecast_date_utc", "forecast_hour_utc",
        "temp_max", "temp_min", "temp_avg",
        "precipitation", "rain", "snow", "precip_prob",
         "humidity", "wind_speed", "wind_deg",
        "clouds","weather_code", "weather_description", "weather_icon",
        "sunrise", "sunset"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Your custom sanitizer
    def _sanitize(records):
        return [
            {k: (None if v is pd.NaT or v == "NaT" else v) for k, v in row.items()}
            for row in records
        ]

    stmt = """
    INSERT INTO silver_weather_forecast_data (
        api_name, place_name, ingest_date, ingest_hour,
        original_ts, forecast_date_utc, forecast_hour_utc, 
        temp_max, temp_min, temp_avg,
        precipitation, rain, snow, precip_prob,
         humidity, wind_speed, wind_deg,
        clouds, weather_code, weather_description, weather_icon,
        sunrise, sunset
    ) VALUES (
        :api_name, :place_name, :ingest_date, :ingest_hour,
        :original_ts, :forecast_date_utc, :forecast_hour_utc, 
        :temp_max, :temp_min, :temp_avg,
        :precipitation, :rain, :snow, :precip_prob,
         :humidity, :wind_speed, :wind_deg,
        :clouds, :weather_code, :weather_description, :weather_icon,
        :sunrise, :sunset
    )
    ON CONFLICT (api_name, place_name, ingest_date, ingest_hour, forecast_date_utc, forecast_hour_utc) DO NOTHING
    """

    batch_size = 1000
    total_inserted = 0
    total_skipped = 0

    try:
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start + batch_size]
            values = _sanitize(batch.to_dict(orient="records"))

            with engine.begin() as connection:
                result = connection.execute(text(stmt), values)
                inserted = result.rowcount or 0
                skipped = len(values) - inserted

            total_inserted += inserted
            total_skipped += skipped

            logger.info(
                "Batch %s loaded | inserted=%s | skipped=%s | total=%s",
                start // batch_size + 1, inserted, skipped, len(values)
            )

        logger.info(
            "Silver data loaded successfully | total_inserted=%s | total_skipped=%s | total_rows=%s",
            total_inserted, total_skipped, len(df)
        )

    except SQLAlchemyError as e:
        logger.exception("Failed to load silver data: %s", e)
        raise

# import pandas as pd
# from prefect import get_run_logger
# from sqlalchemy import text
# from sqlalchemy.exc import SQLAlchemyError
#
#
# def load_silver_data_to_postgres_worker(df: pd.DataFrame, engine):
#     logger = get_run_logger()
#
#     if df is None or df.empty:
#         logger.warning("Silver load skipped: empty dataframe")
#         return
#
#     # Ensure source column exists
#     if "api_name" not in df.columns:
#         df["api_name"] = df.get("source_api")
#
#     cols = [
#         "api_name", "place_name", "ingest_date", "ingest_hour",
#         "forecast_date", "forecast_time",
#         "temp_max", "temp_min", "temp_avg",
#         "precipitation", "rain", "snow", "pop",
#         "wind_speed", "wind_deg",
#         "clouds", "humidity",
#         "weather_code", "weather_main", "weather_description", "weather_icon",
#         "sunrise", "sunset"
#     ]
#
#     for col in cols:
#         if col not in df.columns:
#             df[col] = None
#
#     stmt = """
#     INSERT INTO silver_weather_forecast_data (
#         api_name, place_name, ingest_date, ingest_hour,
#         forecast_date, forecast_time,
#         temp_max, temp_min, temp_avg,
#         precipitation, rain, snow, pop,
#         wind_speed, wind_deg,
#         clouds, humidity,
#         weather_code, weather_main, weather_description, weather_icon,
#         sunrise, sunset
#     ) VALUES (
#         :api_name, :place_name, :ingest_date, :ingest_hour,
#         :forecast_date, :forecast_time,
#         :temp_max, :temp_min, :temp_avg,
#         :precipitation, :rain, :snow, :pop,
#         :wind_speed, :wind_deg,
#         :clouds, :humidity,
#         :weather_code, :weather_main, :weather_description, :weather_icon,
#         :sunrise, :sunset
#     )
#     ON CONFLICT (api_name, place_name, forecast_date, forecast_time, ingest_date, ingest_hour) DO NOTHING
#
#     """
#
#     def _sanitize(records):
#         return [
#             {k: (None if v is pd.NaT or v == "NaT" else v) for k, v in row.items()}
#             for row in records
#         ]
#
#     try:
#         for start in range(0, len(df), 1000):
#             batch = df.iloc[start:start + 1000]
#             values = _sanitize(batch.to_dict(orient="records"))
#
#             with engine.begin() as connection:
#                 result = connection.execute(text(stmt), values)
#                 inserted = result.rowcount
#                 skipped = len(values) - inserted
#
#             logger.info(
#                 "Batch loaded | inserted=%s | skipped=%s | total=%s",
#                 inserted, skipped, len(values),
#             )
#
#         logger.info(
#             "Silver data loaded successfully",
#             extra={"rows": len(df), "table": "silver_weather_forecast_data"}
#         )
#
#     except SQLAlchemyError:
#         logger.error("Failed to load silver data", exc_info=True)
#         raise
