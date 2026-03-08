from io import BytesIO
from sqlite3 import OperationalError

import pandas as pd
import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from decouple import config
from prefect import task
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.extract_silver_data import fetch_silver_data_postgres


def get_hourly_blobs_for_day(year, month, day, fs_client) -> pd.DataFrame:
    df = pd.DataFrame()

    day_path = f"{config('BASE_DIR_GOLD')}/{year}/{month}/{day}"
    directory_client = fs_client.get_directory_client(day_path)
    try:
        paths = list(directory_client.get_paths())
    except ResourceNotFoundError as e:
        raise RuntimeError(
            f"Expected partition missing: {day_path}"
        ) from e

    if not paths:
        raise RuntimeError(
            f"Partition exists but contains no files: {day_path}"
        )
    files = [path.name for path in paths if not path.is_directory and path.name.endswith(".parquet")]
    files = sorted(files)

    dfs = []
    for file in files:
        try:  # just safety check if file is deleted between listing and fetching
            file_client = fs_client.get_file_client(file)
            downloaded_bytes = file_client.download_file().readall()
        except ResourceNotFoundError:
            raise FileNotFoundError(f"Parquet file not found: {file}")

        curr_df = pd.read_parquet(BytesIO(downloaded_bytes))
        dfs.append(curr_df)

    df = pd.concat(dfs, ignore_index=True)
    return df


def get_hourly_data_postgres(date, engine, chunksize=100_000):
    """
    Fetch gold data from Postgres in chunks, sanitize UUIDs.

    Raises ValueError if no data.
    """
    query = """
          SELECT *
            FROM gold_daily_forecast_data
           WHERE ingest_date = %(date)s
        ORDER BY ingest_date DESC;
    """
    chunks = []
    for chunk in pd.read_sql(query, engine, params={"date": date}, chunksize=chunksize):
        chunks.append(chunk)

    if not chunks:
        raise ValueError(f"No gold data found for {date}")

    return pd.concat(chunks, ignore_index=True)


def get_daily_blobs_for_week(week_dates, fs_client) -> tuple[pd.DataFrame, list[str]]:
    """Fetch a single d.parquet file from base_dir/y/m/d.parquet"""
    logger = get_logger()
    all_days_dfs = []
    missing_days = []

    for current_ts in week_dates:
        year = current_ts.format("YYYY")
        month = current_ts.format("MM")
        day = current_ts.format("DD")
        file_path = f"{config('BASE_DIR_WEEKLY_SUMM_GOLD')}/{year}/{month}/{day}.parquet"

        try:
            file_client = fs_client.get_file_client(file_path)
            downloaded_bytes = file_client.download_file().readall()
            df = pd.read_parquet(BytesIO(downloaded_bytes))

            if df.empty:
                logger.warning(f"Empty parquet for {year}-{month}-{day}, treating as missing")
                missing_days.append(f"{year}-{month}-{day}")
            else:
                all_days_dfs.append(df)

        except ResourceNotFoundError:
            logger.warning(f"Blob not found: {file_path}")
            missing_days.append(f"{year}-{month}-{day}")

    combined = pd.concat(all_days_dfs) if all_days_dfs else pd.DataFrame()
    return combined, missing_days



