import psycopg
from decouple import config

from src.helpers.logging_helper.combine_loggers_helper import get_logger
from logging_config import setup_logging

setup_logging()
logger = get_logger()


def load_transformed_api_data_to_postgres(data):
    """
    Load transformed data into Postgres.
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

    forecast_date = data["date"]
    place_name = data["place_name"]
    source = data["api"]
    hour = data["hour"]
    temperature = data["temperature"]
    humidity = data["humidity"]
    wind_speed = data["wind_speed"]
    wind_direction = data["wind_direction"]

    # Use a unique constraint for idempotency
    # Make sure table has UNIQUE(source, payload->>'id') or similar
    try:
        with psycopg.connect(connection_string) as conn:
            with conn.cursor() as cur:
                # INSERT ... ON CONFLICT DO NOTHING
                cur.execute(
                    """
                    INSERT INTO raw_json_weather_api_data
                        (forecast_date, place_name, source, hour, temperature, humidity, wind_speed, wind_direction)   # add more metrics if need after ingest raw data
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (forecast_date, place_name, source, hour, temperature) DO NOTHING
                    """,
                    (forecast_date, place_name, source, hour, temperature, humidity, wind_speed, wind_direction)
                )

                # Check how many rows were inserted
                inserted_rows = cur.rowcount

        if inserted_rows == 0:
            logger.info(
                "Postgres insert skipped: record already exists | forecast_date= %s - place_name= %s - source= %s - hour= %s",
                forecast_date,
                place_name,
                source,
                hour,
                extra={
                    "storage": "postgres DB",
                    "path": f"{config("DB_NAME_FOR_TRANSFORMED_WEATHER_API_DATA")}/{forecast_date}/{place_name}/{source}/{hour}",
                    "reason": "exist",
                    "inserted": False,
                }
            )
            return {"inserted": False, "source": source, "reason": "already_exists"}
        else:
            logger.info(
                "Data loaded into Postgres successfully | source=%s | place_name=%s | ingest_date=%s | ingest_hour=%s",
                forecast_date,
                place_name,
                source,
                hour,
                extra={
                    "storage": "postgres DB",
                    "path": f"{config("DB_NAME_FOR_TRANSFORMED_WEATHER_API_DATA")}|{forecast_date}|{place_name}|{source}|{hour}",
                    "result": "inserted",
                    "inserted": True,
                }
            )
            return {"inserted": True, "source": source, "result": "inserted"}

    except Exception as e:
        logger.error("Postgres load failed | source=%s/place_name=%s/ingest_date=%s/ingest_hour=%s",
                     forecast_date,
                     place_name,
                     source,
                     hour,
                     exc_info=True,
                     extra={
                         "storage": "postgres DB",
                         "path": f"{config("DB_NAME_FOR_TRANSFORMED_WEATHER_API_DATA")}|{forecast_date}|{place_name}|{source}|{hour}",
                         "result": "failed",
                         "inserted": False,
                     }
                     )
        raise
