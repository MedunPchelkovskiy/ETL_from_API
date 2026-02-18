import pandas as pd
import pendulum
from src.helpers.logging_helpers.combine_loggers_helper import get_logger


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



def get_last_processed_timestamp(conn, pipeline_name: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_processed_timestamp
            FROM pipeline_metadata
            WHERE pipeline_name = %s
            """,
            (pipeline_name,)
        )
        row = cur.fetchone()

    return row[0] if row else None


def update_last_processed_timestamp(conn, pipeline_name: str, new_ts):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_metadata (pipeline_name, last_processed_timestamp)
            VALUES (%s, %s)
            ON CONFLICT (pipeline_name)
            DO UPDATE SET
                last_processed_timestamp = EXCLUDED.last_processed_timestamp,
                updated_at = NOW()
            """,
            (pipeline_name, new_ts)
        )
    conn.commit()
