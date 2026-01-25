import pandas as pd
from sqlalchemy.exc import OperationalError
from src.helpers.logging_helper.combine_loggers_helper import get_logger


def extract_bronze_data_from_postgres(conn, date, hour):
    """
    Fetch all raw JSON rows for given date/hour.
    DB table can be make dynamic, if needed!!
    """
    logger = get_logger()
    query = f"""
        SELECT *
        FROM raw_json_weather_api_data                
        WHERE ingest_date = '{date}' 
          AND ingest_hour >= {hour}
        ORDER BY ingest_date DESC, ingest_hour DESC;
    """
    try:
        raw_df = pd.read_sql_query(query, conn)
        logger.info("Fetching raw JSON data from raw_json_weather_api_data table successfully")
        return raw_df      # Return data frame with last fresh api calls
    except OperationalError as e:
        logger.error("Database query failed: %s", e)
        raise

# import psycopg
# from psycopg.rows import dict_row
# from decouple import config
#
# from logging_config import setup_logging
# from src.helpers.logging_helper.combine_loggers_helper import get_logger
#
# setup_logging()
# logger = get_logger()
#
#
# def extract_bronze_data_from_postgres():
#     username = config("DB_USER")
#     password = config("DB_PASSWORD")
#     host = config("DB_HOST")
#     port = config("DB_PORT")
#     database = config("DB_NAME_FOR_RAW_WEATHER_API_DATA")
#     connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
#
#     try:
#         with psycopg.connect(connection_string) as conn:
#             with conn.cursor(row_factory=dict_row) as cur:
#                 cur.execute(
#                     """
#                       SELECT *
#                         FROM raw_json_weather_api_data
#                        WHERE ingest_date = CURRENT_DATE
#                          AND ingest_hour >= EXTRACT(HOUR FROM NOW())
#                     ORDER BY ingest_date DESC, ingest_hour DESC;
#                     """
#                 )
#
#                 fresh_data = cur.fetchall()
#             logger.info(
#                 "Postgres extract from bronze layer successfully ",
#                 extra={
#                     "storage": "postgres DB",
#                     "path": f"{config("DB_NAME_FOR_RAW_WEATHER_API_DATA")}",
#                     "result": "extracted",
#                     "inserted": True,
#                 }
#             )
#         return fresh_data
#
#     except Exception as e:
#         logger.error("Postgres extract from bronze failed | source=%s/place_name=%s/ingest_date=%s/ingest_hour=%s",
#                      exc_info=True,
#                      extra={
#                          "storage": "postgres DB",
#                          "path": f"{config("DB_NAME_FOR_RAW_WEATHER_API_DATA")}",
#                          "result": "failed",
#                          "inserted": False,
#                      }
#                      )
#         raise
