import pendulum
import psycopg2
from decouple import config

from src.helpers.observability_helpers.pipeline_config import PIPELINE_CONFIG


def get_pending_work(
        processing_level: str,
        statuses: list[str],
        error_types: list,  # может да съдържа None → OR error_type IS NULL
) -> list[pendulum.DateTime]:
    """
    Returns list of partition_dates (as pendulum.DateTime UTC)
    that match the given statuses and error_types.

    None in error_types → includes rows where error_type IS NULL.
    """
    conn = psycopg2.connect(config("DB_CONN_RAW"))
    max_retries = {
        "gold_weekly": 3,  # 3 пускания = 3 седмици = приемаме загубата
        "gold_daily": 5,
        "bronze": 10,  # API issues се оправят по-бързо
    }

    try:
        # Split None from real values
        real_errors = [e for e in error_types if e is not None]
        include_null = None in error_types

        status_placeholders = ",".join(["%s"] * len(statuses))

        # Build error_type clause
        if real_errors and include_null:
            error_placeholders = ",".join(["%s"] * len(real_errors))
            error_clause = f"AND (error_type IN ({error_placeholders}) OR error_type IS NULL)"
            error_params = real_errors
        elif real_errors:
            error_placeholders = ",".join(["%s"] * len(real_errors))
            error_clause = f"AND error_type IN ({error_placeholders})"
            error_params = real_errors
        elif include_null:
            error_clause = "AND error_type IS NULL"
            error_params = []
        else:
            error_clause = ""
            error_params = []

        query = f"""
            SELECT partition_date
            FROM processing_state
            WHERE processing_level = %s
              AND status IN ({status_placeholders})
              AND retry_count < %s   -- ← ново
              {error_clause}
            ORDER BY partition_date
        """
        params = (processing_level, *statuses, PIPELINE_CONFIG[processing_level]["max_retries"], *error_params)

        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        return [
            pendulum.datetime(row[0].year, row[0].month, row[0].day, tz="UTC")
            for row in rows
        ]

    finally:
        conn.close()
