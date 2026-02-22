import io

import pandas as pd
from decouple import config
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.clients.datalake_client import fs_client
from src.helpers.gold.load import upload_gold_bytes, update_last_processed_timestamp
from src.helpers.logging_helpers.combine_loggers_helper import get_logger


def load_gold_data_to_azure_worker(pipeline_name, gold_result: list):
    """
    Converts a Pandas DataFrame to Parquet bytes and uploads to Azure using the base uploader.
    """
    for ts, df in gold_result:
        year_str = ts.format("YYYY")
        month_str = ts.format("MM")
        day_str = ts.format("DD")
        hour_str = ts.format("HH")

        year_folder_name = f"{year_str}"
        month_folder_name = f"{month_str}"
        day_folder_name = f"{day_str}"
        file_name = f"{hour_str}.parquet"
        # Convert to Parquet bytes
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy")
        parquet_bytes = parquet_buffer.getvalue()
        upload_gold_bytes(fs_client, config("BASE_DIR_GOLD"), year_folder_name, month_folder_name,
                          day_folder_name,
                          file_name, parquet_bytes)
        update_last_processed_timestamp(pipeline_name, ts)    #TODO: Make update only if new blob is uploaded! Now it update for run!!!!


def load_gold_daily_data_to_postgres_worker(gold_results:list, engine):
    """
    Loader for gold_daily_forecast_data using custom sanitizer.

    """
    logger = get_logger()
    logger.info("Воркерчето започва да работи")

    for ts, df in gold_results:
        logger.info("DF shape: %s", df.shape)
        logger.info("DF columns: %s", df.columns.tolist())
        logger.info("Фор цикъчето започва да работи")

        if df is None or df.empty:
            logger.warning("Gold worker skip load: empty dataframe")
            continue

        # Ensure required columns
        required_cols = [
            "place_name", "ingest_date", "ingest_hour",
            "forecast_date_utc", "forecast_hour_utc", "generated_at",
            "temp_max", "temp_min", "temp_avg",
            "rain_min", "rain_max", "rain_avg",
            "snow_min", "snow_max", "snow_avg",
            "wind_speed_min", "wind_speed_max", "wind_speed_avg",
            "cloud_cover_min", "cloud_cover_max", "cloud_cover_avg",
            "humidity_min", "humidity_max", "humidity_avg"
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
        INSERT INTO gold_daily_forecast_data (
            place_name, ingest_date, ingest_hour,
            forecast_date_utc, forecast_hour_utc, generated_at,
            temp_max, temp_min, temp_avg,
            rain_min, rain_max, rain_avg,
            snow_min, snow_max, snow_avg,
            wind_speed_min, wind_speed_max, wind_speed_avg,
            cloud_cover_min, cloud_cover_max, cloud_cover_avg,
            humidity_min, humidity_max, humidity_avg
        ) VALUES (
            :place_name, :ingest_date, :ingest_hour,
            :forecast_date_utc, :forecast_hour_utc, :generated_at,
            :temp_max, :temp_min, :temp_avg,
            :rain_min, :rain_max, :rain_avg,
            :snow_min, :snow_max, :snow_avg,
            :wind_speed_min, :wind_speed_max, :wind_speed_avg,
            :cloud_cover_min, :cloud_cover_max, :cloud_cover_avg,
            :humidity_min, :humidity_max, :humidity_avg
        )   
        ON CONFLICT (place_name, forecast_date_utc, generated_at) DO NOTHING
        """

        batch_size = 1000
        total_inserted = 0
        total_skipped = 0

        try:
            for start in range(0, len(df), batch_size):
                batch = df.iloc[start:start + batch_size]
                values = _sanitize(batch.to_dict(orient="records"))
                print(df["generated_at"].unique())
                print(len(df["generated_at"].unique()))
                with engine.begin() as connection:
                    result = connection.execute(text(stmt), values)
                with engine.begin() as connection:
                    count = connection.execute(
                        text("SELECT COUNT(*) FROM gold_daily_forecast_data")
                    ).scalar()

                    print("TOTAL ROWS IN DB:", count)
                    inserted = result.rowcount or 0
                    skipped = len(values) - inserted

                total_inserted += inserted
                total_skipped += skipped

                logger.info(
                    "Batch %s loaded | inserted=%s | skipped=%s | total=%s",
                    start // batch_size + 1, inserted, skipped, len(values)
                )

            logger.info(
                "Daily data loaded successfully | total_inserted=%s | total_skipped=%s | total_rows=%s",
                total_inserted, total_skipped, len(df)
            )

        except SQLAlchemyError as e:
            logger.exception("Failed to load daily data: %s", e)
            raise


def load_five_day_data_to_postgres_worker(df: pd.DataFrame, engine):
    """
    Loader for gold_daily_forecast_data using custom sanitizer.

    Args:
        df (pd.DataFrame): Data to insert.
        engine: SQLAlchemy Engine connected to Postgres.
    """
    logger = get_logger()

    if df is None or df.empty:
        logger.warning("Gold load skipped: empty dataframe")
        return

    # Ensure required columns
    required_cols = [
        "place_name", "ingest_date", "ingest_hour",
        "forecast_date_utc", "forecast_hour_utc",
        "temp_max", "temp_min", "temp_avg",
        "rain_min", "rain_max", "rain_avg",
        "snow_min", "snow_max", "snow_avg",
        "wind_speed_min", "wind_speed_max", "wind_speed_avg",
        "cloud_cover_min", "cloud_cover_max", "cloud_cover_avg",
        "humidity_min", "humidity_max", "humidity_avg"
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
    INSERT INTO gold_five_day_forecast_data (
        place_name, ingest_date, ingest_hour,
        forecast_date_utc, forecast_hour_utc,
        temp_max, temp_min, temp_avg,
        rain_min, rain_max, rain_avg,
        snow_min, snow_max, snow_avg,
        wind_speed_min, wind_speed_max, wind_speed_avg,
        cloud_cover_min, cloud_cover_max, cloud_cover_avg,
        humidity_min, humidity_max, humidity_avg
    ) VALUES (
        :place_name, :ingest_date, :ingest_hour,
        :forecast_date_utc, :forecast_hour_utc,
        :temp_max, :temp_min, :temp_avg,
        :rain_min, :rain_max, :rain_avg,
        :snow_min, :snow_max, :snow_avg,
        :wind_speed_min, :wind_speed_max, :wind_speed_avg,
        :cloud_cover_min, :cloud_cover_max, :cloud_cover_avg,
        :humidity_min, :humidity_max, :humidity_avg
    )   
    ON CONFLICT (place_name, forecast_date, forecast_time) DO NOTHING;
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
            "Daily data loaded successfully | total_inserted=%s | total_skipped=%s | total_rows=%s",
            total_inserted, total_skipped, len(df)
        )

    except SQLAlchemyError as e:
        logger.exception("Failed to load daily data: %s", e)
        raise
