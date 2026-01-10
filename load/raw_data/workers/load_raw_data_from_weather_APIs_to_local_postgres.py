import psycopg
from decouple import config
from psycopg.types.json import Json


def load_raw_api_data_to_postgres(data):
    username = config("DB_USER")
    password = config("DB_PASSWORD")
    host = config("DB_HOST")
    port = config("DB_PORT")
    database = config("DB_NAME_FOR_RAW_WEATHER_API_DATA")

    # Create connection string
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

    data_to_insert = data["data"]
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw_json_weather_api_data (source, payload)
                VALUES (%s, %s)
                """,
                (data["api"], Json(data_to_insert))
            )

    print(f"Data loaded into raw_json_weather_api_data table in PostgresSQL successfully!")

# import json
#
# from decouple import config
# from sqlalchemy import create_engine
#
#
# def load_raw_api_data_to_postgres(data, table_name):
#     # Database credentials (replace with actual values)
#     username = config("DB_USER")
#     password = config("DB_PASSWORD")
#     host = config("DB_HOST")
#     port = config("DB_PORT")
#     database = config("DB_NAME_FOR_RAW_WEATHER_API_DATA")
#
#     # Create connection string
#     connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
#
#     # Create an engine to manage the connection to the PostgreSQL database
#     engine = create_engine(connection_string)
#
#     # Load the DataFrame into the PostgresSQL table (Replace existing table if exists)
#     json_to_db = json.dumps(data)
#     json_to_db.to_sql(table_name, engine, if_exists='append', index=False)
#     print(f"Data loaded into {table_name} table in PostgresSQL successfully!")
