from io import BytesIO

import pandas as pd
from decouple import config

from src.clients.datalake_client import fs_client
from src.helpers.logging_helpers.combine_loggers_helper import get_logger


def fetch_silver_parquet_blob(year, month, day, hour):
    dir_path = f"{config('BASE_DIR_SILVER')}/{year}/{month}/{day}"

    directory_client = fs_client.get_directory_client(dir_path)
    file_client = directory_client.get_file_client(f"{hour}.parquet")

    # download as bytes
    downloaded_bytes = file_client.download_file().readall()

    # wrap in BytesIO so Pandas can read it
    return BytesIO(downloaded_bytes)


def fetch_silver_data_postgres(date, hour_int, engine):
    """
    Worker function to fetch silver weather data from Postgres.

    Raises:
        ValueError: if no data is found for the given date and hour
    """
    logger = get_logger()

    query = """
        SELECT *
        FROM silver_weather_forecast_data
        WHERE ingest_date = %(date)s
          AND ingest_hour >= %(hour)s
        ORDER BY ingest_date DESC, ingest_hour DESC;
    """

    try:
        silver_df = pd.read_sql(
            query,
            engine,
            params={"date": date, "hour": hour_int}
        )

        # Check if DataFrame is empty → fail task in pipeline
        if silver_df.empty:
            raise ValueError(f"No silver data found for {date}-{hour_int}")

        return silver_df

    except Exception as e:
        logger.error(f"Failed to fetch silver data: {e}")
        raise
