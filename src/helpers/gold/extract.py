from typing import Optional

import pandas as pd
import pendulum
import psycopg2
from decouple import config


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
    Връща последния успешно обработен timestamp за даден pipeline.
    Ако няма запис, връща None.
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
