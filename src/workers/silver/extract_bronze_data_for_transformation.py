import json

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from src.helpers.bronze.api_location_mapper import api_locations
from src.helpers.logging_helpers.combine_loggers_helper import get_logger


def download_json_from_adls_worker(azure_fs_client, base_dir, date, hour):
    logger = get_logger()
    records = []

    for api_name, places in api_locations.items():
        for place in places:
            place_name = place[0]
            dir_path = f"{base_dir}/{date}_{api_name}_{place_name}"

            directory_client = azure_fs_client.get_directory_client(dir_path)
            file_client = directory_client.get_file_client(f"{hour}.json")

            stream = file_client.download_file()
            content = stream.readall()

            records.append({
                "source": api_name,
                "place_name": place_name,
                "ingest_date": date,  # from folder
                "ingest_hour": int(hour),  # from filename
                "payload": json.loads(content)
            })

    return records


def extract_bronze_data_from_postgres_worker(engine, date, hour):
    """
    Fetch all raw JSON rows for given date/hour.
    DB table can be make dynamic, if needed!!
    """
    logger = get_logger()

    try:
        query = text("""
               SELECT
                   source,
                   place_name,
                   ingest_date,
                   ingest_hour,
                   payload
               FROM raw_json_weather_api_data
               WHERE ingest_date = :date
                 AND ingest_hour = :hour
           """)

        with engine.connect() as conn:
            result = conn.execute(query, {"date": date, "hour": hour})
            records = [dict(row) for row in result.mappings()]

        return records

    except OperationalError as e:
        logger.error("Database query failed: %s", e)
        raise
