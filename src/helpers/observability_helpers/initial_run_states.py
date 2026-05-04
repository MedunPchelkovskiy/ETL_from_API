from typing import List

import pandas as pd
import pendulum
import psycopg2
from azure.core.exceptions import ResourceNotFoundError
from decouple import config

from src.helpers.logging_helpers.combine_loggers_helper import get_logger


def generate_dates(start_date, end_date, grain: str ) -> List[pendulum.DateTime]:
    """
    Generates partitions based on grain: daily / weekly / monthly
    """

    start_date = pendulum.instance(start_date).start_of(grain)
    end_date = pendulum.instance(end_date).start_of(grain)

    dates = []

    current = start_date

    while current < end_date:        # TODO: note that end_date is excluded!!!!!
        dates.append(current)
        if grain == "hour":
            current = current.add(hours=1)

        elif grain == "day":
            current = current.add(days=1)

        elif grain == "week":
            current = current.add(weeks=1)

        elif grain == "month":
            current = current.add(months=1)

        else:
            raise ValueError(f"Unsupported grain: {grain}")

    return dates


def find_existing_daily_blobs(week_dates: list, fs_client) -> list[tuple]:
    """
    Ultra-fast reconciliation:
    - NO parquet download
    - NO pandas
    - ONLY blob existence check

    Returns: [(date, status)]
    """

    logger = get_logger()
    results = []

    for current_ts in week_dates:

        year = current_ts.format("YYYY")
        month = current_ts.format("MM")
        day = current_ts.format("DD")

        file_path = f"{config('BASE_DIR_DAILY_SUMM_GOLD')}/{year}/{month}/{day}.parquet"

        try:
            file_client = fs_client.get_file_client(file_path)

            # ONLY metadata check (no download)
            file_client.get_file_properties()

            status = "success"

        except ResourceNotFoundError:
            status = "missing"

        results.append((current_ts, status))

    return results


def find_existing_daily_postgres(week_dates: list[pendulum.DateTime], engine) -> list[tuple]:
    """
    Fast reconciliation for Postgres:
    - no full dataset loading
    - only checks existence per date
    """

    logger = get_logger()
    results = []

    query = """
        SELECT forecast_date_utc
        FROM gold_daily_summarized_data
        WHERE forecast_date_utc BETWEEN %s AND %s
        GROUP BY forecast_date_utc;
    """

    for current_ts in week_dates:

        current_date = current_ts.date()

        df = pd.read_sql(
            query,
            engine,
            params={"date": current_date},
        )

        if df.empty:
            status = "missing"
        else:
            status = "success"

        results.append((current_ts, status))

    return results


def upsert_state_fn(
        processing_level: str,
        partition_date: pendulum.DateTime,
        status: str,
        expected_count: int = None,
        actual_count: int = None,
        error_message: str = None,
):
    """
    Inserts or updates processing_state row (idempotent)
    """
    conn = psycopg2.connect(
        config("DB_CONN_RAW")
    )

    query = """
        INSERT INTO processing_state (
            processing_level,
            partition_date,
            status,
            expected_count,
            actual_count,
            completeness_ratio,
            is_acceptable,
            updated_at,
            error_message
        )
        VALUES (
            %s, %s, %s,
            %s, %s,
            CASE 
                WHEN %s IS NOT NULL AND %s IS NOT NULL 
                THEN %s::float / NULLIF(%s, 0)
                ELSE NULL
            END,
            CASE 
                WHEN %s IS NOT NULL AND %s IS NOT NULL 
                THEN (%s::float / NULLIF(%s, 0)) >= 1
                ELSE NULL
            END,
            NOW(),
            %s
        )
        ON CONFLICT (processing_level, partition_date)
        DO UPDATE SET
            status = EXCLUDED.status,
            retry_count = processing_state.retry_count + 1
            expected_count = EXCLUDED.expected_count,
            actual_count = EXCLUDED.actual_count,
            completeness_ratio = EXCLUDED.completeness_ratio,
            is_acceptable = EXCLUDED.is_acceptable,
            updated_at = NOW(),
            error_message = EXCLUDED.error_message;
    """

    params = (
        processing_level,
        partition_date.date(),
        status,

        expected_count,
        actual_count,

        actual_count, expected_count,
        actual_count, expected_count,

        actual_count, expected_count,
        actual_count, expected_count,

        error_message,
    )

    with conn.cursor() as cur:
        cur.execute(query, params)

    conn.commit()


def reconcile_processing_state(
    pipeline_name: str,
    start_date,
    end_date,
    fs_client,
    engine,
    conn,
):
    """
    Production reconciliation:
    Azure + Postgres → merge → processing_state
    """

    # 1. generate expected partitions
    expected_dates = generate_dates(start_date, end_date)

    # 2. parallel scans (logical, not async)
    azure_state = find_existing_daily_blobs(expected_dates, fs_client)
    postgres_state = find_existing_daily_postgres(expected_dates, engine)

    # 3. build lookup maps
    azure_map = {d: s for d, s in azure_state}
    postgres_map = {d: s for d, s in postgres_state}

    # 4. merge logic (deterministic)
    for d in expected_dates:

        a = azure_map.get(d, "missing")
        p = postgres_map.get(d, "missing")

        if a == "success" or p == "success":
            status = "success"

        elif a == "missing" and p == "missing":
            status = "missing"

        else:
            status = "partial"

        upsert_state_fn(
            conn=conn,
            processing_level=pipeline_name,
            partition_date=d,
            status=status,
            expected_count=24,
            actual_count=24 if status == "success" else 0,
        )