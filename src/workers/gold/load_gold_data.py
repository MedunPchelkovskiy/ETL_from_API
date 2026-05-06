import io

import pandas as pd
import pendulum
from decouple import config
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.clients.datalake_client import fs_client
from src.helpers.gold.load import update_last_processed_timestamp, upload_parquet_bytes
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
        upload_parquet_bytes(fs_client, config("BASE_DIR_GOLD"), [year_folder_name, month_folder_name,
                                                                  day_folder_name,
                                                                  file_name], parquet_bytes)
        update_last_processed_timestamp(pipeline_name,
                                        ts)  # TODO: Make update only if new blob is uploaded! Now it update for run!!!!


def load_gold_daily_data_to_postgres_worker(gold_results: list, engine):
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


def load_gold_five_day_data_to_azure_worker(pipeline_name, gold_result: list):
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
        upload_parquet_bytes(fs_client, config("BASE_DIR_FIVE_DAY_GOLD"), [year_folder_name, month_folder_name,
                                                                           day_folder_name,
                                                                           file_name], parquet_bytes)
        update_last_processed_timestamp(pipeline_name,
                                        ts)  # TODO: Make update only if new blob is uploaded! Now it update for run!!!!


def load_five_day_data_to_postgres_worker(fd_gold_results, engine):
    """
    Loader for gold_five_day_forecast_data using custom sanitizer.

    Args:
        fd_gold_results (list): Data to insert.
        engine: SQLAlchemy Engine connected to Postgres.
    """
    logger = get_logger()
    logger.info("Воркерчето за пет дни започва да работи")

    for ts, df in fd_gold_results:
        logger.info("DF shape: %s", df.shape)
        logger.info("DF columns: %s", df.columns.tolist())
        logger.info("Фор цикъчето за пет дни започва да работи")

        if df is None or df.empty:
            logger.warning("Gold five day worker skip load: empty dataframe")
            continue

        # Ensure required columns
        required_cols = [
            "place_name", "ingest_date", "ingest_hour",
            "forecast_date_utc", "generated_at",
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
            forecast_date_utc, generated_at,
            temp_max, temp_min, temp_avg,
            rain_min, rain_max, rain_avg,
            snow_min, snow_max, snow_avg,
            wind_speed_min, wind_speed_max, wind_speed_avg,
            cloud_cover_min, cloud_cover_max, cloud_cover_avg,
            humidity_min, humidity_max, humidity_avg
        ) VALUES (
            :place_name, :ingest_date, :ingest_hour,
            :forecast_date_utc, :generated_at,
            :temp_max, :temp_min, :temp_avg,
            :rain_min, :rain_max, :rain_avg,
            :snow_min, :snow_max, :snow_avg,
            :wind_speed_min, :wind_speed_max, :wind_speed_avg,
            :cloud_cover_min, :cloud_cover_max, :cloud_cover_avg,
            :humidity_min, :humidity_max, :humidity_avg
        )   
        ON CONFLICT (place_name, forecast_date_utc, ingest_date, ingest_hour) DO NOTHING
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


def load_daily_summ_data_to_azure_worker(pipeline_name, gold_result: list):
    """
    Converts a Pandas DataFrame to Parquet bytes and uploads to Azure using the base uploader.
    """
    logger = get_logger()
    logger.info(f"type(gold_results) = {type(gold_result)}")
    logger.info(f"gold_results sample = {gold_result[:1]}")
    for ts, df in gold_result:
        year_str = ts.format("YYYY")
        month_str = ts.format("MM")
        day_str = ts.format("DD")

        year_folder_name = f"{year_str}"
        month_folder_name = f"{month_str}"
        day_file_name = f"{day_str}.parquet"
        # Convert to Parquet bytes
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy")
        parquet_bytes = parquet_buffer.getvalue()
        upload_parquet_bytes(fs_client, config("BASE_DIR_DAILY_SUMM_GOLD"), [year_folder_name, month_folder_name,
                                                                             day_file_name],
                             parquet_bytes)
        update_last_processed_timestamp(pipeline_name,
                                        ts)  # TODO: Make update only if new blob is uploaded! Now it update for run!!!!


def load_gold_daily_summ_data_to_postgres_worker(gold_results: list[tuple[pendulum.DateTime, pd.DataFrame]], engine):
    """
    Loader for gold_daily_forecast_data using custom sanitizer.

    """
    logger = get_logger()

    for ts, df in gold_results:
        logger.info("DF shape: %s", df.shape)
        logger.info("DF columns: %s", df.columns.tolist())

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
        INSERT INTO gold_daily_summarized_data (
            place_name, ingest_date, ingest_hour,
            forecast_date_utc, generated_at,
            temp_max, temp_min, temp_avg,
            rain_min, rain_max, rain_avg,
            snow_min, snow_max, snow_avg,
            wind_speed_min, wind_speed_max, wind_speed_avg,
            cloud_cover_min, cloud_cover_max, cloud_cover_avg,
            humidity_min, humidity_max, humidity_avg
        ) VALUES (
            :place_name, :ingest_date, :ingest_hour,
            :forecast_date_utc, :generated_at,
            :temp_max, :temp_min, :temp_avg,
            :rain_min, :rain_max, :rain_avg,
            :snow_min, :snow_max, :snow_avg,
            :wind_speed_min, :wind_speed_max, :wind_speed_avg,
            :cloud_cover_min, :cloud_cover_max, :cloud_cover_avg,
            :humidity_min, :humidity_max, :humidity_avg
        )   
        ON CONFLICT (place_name, forecast_date_utc) DO NOTHING
        """

        batch_size = int(1000)
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


def load_weekly_summ_data_to_azure_worker(pipeline_name, week):
    """
    Converts a Pandas DataFrame to Parquet bytes and uploads to Azure using the base uploader.
    """
    logger = get_logger()

    week_number = int(week["week_number"].iloc[0])
    year_str = str(week["year"].iloc[0])
    file_name = f"W{week_number:02d}.parquet"
    # Convert to Parquet bytes
    parquet_buffer = io.BytesIO()
    week.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy")
    parquet_bytes = parquet_buffer.getvalue()
    upload_parquet_bytes(fs_client, config("BASE_DIR_WEEKLY_SUMM_GOLD"), [year_str,
                                                                          file_name],
                         parquet_bytes)


def load_gold_weekly_summ_data_to_postgres_worker(
        engine,
        all_weeks_summ: list[pd.DataFrame],
):
    """
    Loader for gold_weekly_summarized_data.
    Expects pre-validated DataFrames from the Gold layer.
    Demonstrates batch INSERT with ON CONFLICT for portfolio scalability showcase.
    """
    logger = get_logger()

    stmt = text("""
        INSERT INTO gold_weekly_summarized_data (
            place_name, generated_at,
            week_number, year, week_start,
            temp_max, temp_min, temp_avg,
            rain_min, rain_max, rain_avg,
            snow_min, snow_max, snow_avg,
            wind_speed_min, wind_speed_max, wind_speed_avg,
            cloud_cover_min, cloud_cover_max, cloud_cover_avg,
            humidity_min, humidity_max, humidity_avg
        ) VALUES (
            :place_name, :generated_at,
            :week_number, :year, :week_start,
            :temp_max, :temp_min, :temp_avg,
            :rain_min, :rain_max, :rain_avg,
            :snow_min, :snow_max, :snow_avg,
            :wind_speed_min, :wind_speed_max, :wind_speed_avg,
            :cloud_cover_min, :cloud_cover_max, :cloud_cover_avg,
            :humidity_min, :humidity_max, :humidity_avg
        )
        ON CONFLICT (place_name, year, week_number) DO NOTHING
    """)

    batch_size = int(1000)

    for df in all_weeks_summ:
        if df is None or df.empty:
            logger.warning("Gold weekly summ worker skip load: empty dataframe")
            continue
        week_number = df['week_number'].iloc[0]
        year = df['year'].iloc[0]

        logger.info(
            "Loading week number %s-%s | shape=%s",
            year, week_number, df.shape
        )

        total_inserted = 0
        total_skipped = 0

        try:
            for start in range(0, len(df), batch_size):
                batch = df.iloc[start: start + batch_size]
                values = batch.to_dict(orient="records")

                with engine.begin() as conn:
                    result = conn.execute(stmt, values)
                    inserted = result.rowcount or 0

                skipped = len(values) - inserted
                total_inserted += inserted
                total_skipped += skipped

                logger.info(
                    "Batch %s | inserted=%s | skipped=%s | size=%s",
                    start // batch_size + 1, inserted, skipped, len(values),
                )

            logger.info(
                "Done week number %s | total_inserted=%s | total_skipped=%s | total_rows=%s",
                week_number, total_inserted, total_skipped, len(df),
            )

        except SQLAlchemyError:
            logger.exception("Failed to load weekly summ data for week number %s", week_number)
            raise


def load_monthly_summ_data_to_azure_worker(pipeline_name, month):
    """
    Converts a Pandas DataFrame to Parquet bytes and uploads to Azure using the base uploader.
    """
    logger = get_logger()

    month_number = int(month["month_number"].iloc[0])
    year_str = str(month["year"].iloc[0])
    file_name = f"{month_number:02d}.parquet"
    # Convert to Parquet bytes
    parquet_buffer = io.BytesIO()
    month.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy")
    parquet_bytes = parquet_buffer.getvalue()
    upload_parquet_bytes(fs_client, config("BASE_DIR_MONTHLY_SUMM_GOLD"), [year_str,
                                                                           file_name],
                         parquet_bytes)


def load_gold_monthly_summ_data_to_postgres_worker(
        engine,
        all_months_summ: list[pd.DataFrame],
):
    """
    Loader for gold_monthly_summarized_data.
    Expects pre-validated DataFrames from the Gold layer.
    Demonstrates batch INSERT with ON CONFLICT for portfolio scalability showcase.
    """
    logger = get_logger()

    stmt = text("""
        INSERT INTO gold_monthly_summarized_data (
            place_name, generated_at,
            month_number, year, month_start,
            temp_max, temp_min, temp_avg,
            rain_min, rain_max, rain_avg,
            snow_min, snow_max, snow_avg,
            wind_speed_min, wind_speed_max, wind_speed_avg,
            cloud_cover_min, cloud_cover_max, cloud_cover_avg,
            humidity_min, humidity_max, humidity_avg
        ) VALUES (
            :place_name, :generated_at,
            :month_number, :year, :month_start,
            :temp_max, :temp_min, :temp_avg,
            :rain_min, :rain_max, :rain_avg,
            :snow_min, :snow_max, :snow_avg,
            :wind_speed_min, :wind_speed_max, :wind_speed_avg,
            :cloud_cover_min, :cloud_cover_max, :cloud_cover_avg,
            :humidity_min, :humidity_max, :humidity_avg
        )
        ON CONFLICT (place_name, year, month_number) DO NOTHING
    """)

    batch_size = int(1000)

    for df in all_months_summ:
        if df is None or df.empty:
            logger.warning("Gold monthly summ worker skip load: empty dataframe")
            continue
        month_number = df['month_number'].iloc[0]
        year = df['year'].iloc[0]

        logger.info(
            "Loading month number %s-%s | shape=%s",
            year, month_number, df.shape
        )

        total_inserted = 0
        total_skipped = 0

        try:
            for start in range(0, len(df), batch_size):
                batch = df.iloc[start: start + batch_size]
                values = batch.to_dict(orient="records")

                with engine.begin() as conn:
                    result = conn.execute(stmt, values)
                    inserted = result.rowcount or 0

                skipped = len(values) - inserted
                total_inserted += inserted
                total_skipped += skipped

                logger.info(
                    "Batch %s | inserted=%s | skipped=%s | size=%s",
                    start // batch_size + 1, inserted, skipped, len(values),
                )

            logger.info(
                "Done month number %s | total_inserted=%s | total_skipped=%s | total_rows=%s",
                month_number, total_inserted, total_skipped, len(df),
            )

        except SQLAlchemyError:
            logger.exception("Failed to load monthly summ data for month number %s", month_number)
            raise
