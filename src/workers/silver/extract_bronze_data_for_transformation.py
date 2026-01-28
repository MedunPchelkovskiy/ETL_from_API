import json

import pandas as pd
from azure.core.exceptions import ResourceNotFoundError
from sqlalchemy.exc import OperationalError

from src.helpers.bronze.api_location_mapper import api_locations
from src.helpers.logging_helper.combine_loggers_helper import get_logger


def extract_bronze_data_from_postgres_worker(engine, date, hour):
    """
    Fetch all raw JSON rows for given date/hour.
    DB table can be make dynamic, if needed!!
    """
    logger = get_logger()
    # Parameterized query to prevent SQL injection
    query = """
        SELECT *
        FROM raw_json_weather_api_data
        WHERE ingest_date = %s
          AND ingest_hour >= %s
        ORDER BY ingest_date DESC, ingest_hour DESC;
    """

    try:
        raw_df = pd.read_sql(query, engine, params=(date, hour))
        logger.info("Fetched raw JSON data successfully")
        return raw_df

    except OperationalError as e:
        logger.error("Database query failed: %s", e)
        raise



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
                "payload": json.loads(content)
            })

    return records




















