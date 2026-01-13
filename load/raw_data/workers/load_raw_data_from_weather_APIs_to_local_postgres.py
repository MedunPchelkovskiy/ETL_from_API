from datetime import datetime

import psycopg
from decouple import config
from psycopg.types.json import Json

from logging_config import setup_logging
from logs.combine_loggers_helper import get_logger

setup_logging()
logger = get_logger()


def load_raw_api_data_to_postgres(data, label):
    """
    Load JSON data into Postgres.
    Skips insertion if the same record already exists (idempotent).
    Returns a dict indicating upload result.
    """

    # DB config
    username = config("DB_USER")
    password = config("DB_PASSWORD")
    host = config("DB_HOST")
    port = config("DB_PORT")
    database = config("DB_NAME_FOR_RAW_WEATHER_API_DATA")
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

    source = data["api"]
    place_name = label
    data_to_insert = data["data"]
    ingest_date = datetime.now()
    ingest_hour = datetime.now().hour
    # Use a unique constraint for idempotency
    # Make sure table has UNIQUE(source, payload->>'id') or similar
    try:
        with psycopg.connect(connection_string) as conn:
            with conn.cursor() as cur:
                # Example: INSERT ... ON CONFLICT DO NOTHING
                cur.execute(
                    """
                    INSERT INTO raw_json_weather_api_data
                        (source, place_name, payload, ingest_date, ingest_hour)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (source, place_name, ingest_date, ingest_hour) DO NOTHING
                    """,
                    (source, place_name, Json(data_to_insert), ingest_date, ingest_hour)
                )

                # Check how many rows were inserted
                inserted_rows = cur.rowcount

        if inserted_rows == 0:
            logger.info(
                f"Postgres insert skipped: record already exists | source={source}"
            )
            return {"inserted": False, "source": source, "reason": "already_exists"}
        else:
            logger.info(
                f"Data loaded into Postgres successfully | source={source}"
            )
            return {"inserted": True, "source": source, "reason": "inserted"}

    except Exception as e:
        logger.error(f"Postgres load failed | source={source} | error={e}")
        raise
