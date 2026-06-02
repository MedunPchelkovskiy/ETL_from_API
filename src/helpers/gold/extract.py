from typing import Optional

import pandas as pd
import pendulum
import psycopg2
from decouple import config
from sqlalchemy import create_engine, text

expected_months_map = {
    1: {1, 2, 3},
    2: {4, 5, 6},
    3: {7, 8, 9},
    4: {10, 11, 12},
}

critical_month_map = {
    1: 3,
    2: 6,
    3: 9,
    4: 12,
}


def get_quarter(datetime_obj: pendulum.DateTime):
    return (datetime_obj.month - 1) // 3 + 1


def get_oldest_monthly_date_azure(fs_client, base_dir):
    all_paths = sorted([str(p) for p in fs_client.ls(base_dir, detail=False)])
    oldest_month = all_paths[0]
    parts = oldest_month.split("/")
    year = int(parts[-2])
    month = int(parts[-1].replace(".parquet", ""))

    return year, month

def get_oldest_monthly_date_postgres(db_conn):
    engine = create_engine(db_conn)
    query = """
       SELECT MIN(month_start)
         FROM gold_monthly_summarized_data
   """

    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    if result is None or result[0] is None:
        raise ValueError("No data in gold_monthly_summarized_data")
    oldest_month = result[0]
    year = oldest_month.year
    month = oldest_month.month

    return year, month

def get_last_gold_timestamp_postgres(engine, table_name):
    """
    Връща последния ingest_timestamp от Gold Postgres table
    """
    query = f"SELECT MAX(ingest_timestamp) AS last_ts FROM {table_name}"
    last_ts = pd.read_sql(query, engine)['last_ts'][0]
    if last_ts is None:
        # Ако таблицата е празна
        return pendulum.datetime(1970, 1, 1)
    return pendulum.parse(str(last_ts))


def get_last_processed_timestamp(pipeline_name: str) -> Optional[pendulum.DateTime]:
    """
    Return last successfully processed timestamp for the pipeline.
    If not records, return None.
    """
    conn = psycopg2.connect(config("DB_CONN_RAW"))
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT last_processed_timestamp
                FROM pipeline_metadata
                WHERE pipeline_name = %s
                ORDER BY last_processed_timestamp DESC
                LIMIT 1
                """,
                (pipeline_name,)
            )
            row = cur.fetchone()

        if not row:
            return None

        # Конвертираме в pendulum UTC (production-safe сравнения)
        return pendulum.instance(row[0]).in_timezone("UTC")

    finally:
        conn.close()
