import pendulum
import psycopg2
from azure.core.exceptions import ResourceNotFoundError
from decouple import config
from sqlalchemy import text

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.pipeline_config import PIPELINE_CONFIG
from src.helpers.observability_helpers.initial_run_states import generate_dates


# ── low-level existence checks ────────────────────────────────────────────────

def _blob_path_for_date(ts: pendulum.DateTime, azure_path_env: str) -> str:
    """
    Builds the Azure blob path for a given partition date.
    gold_daily  → year/month/DD.parquet
    gold_weekly → year/W{week_number}.parquet
    """
    base = config(azure_path_env)
    year = ts.format("YYYY")

    if "WEEKLY" in azure_path_env:
        week_number = ts.week_of_year
        return f"{base}/{year}/W{week_number}.parquet"
    else:
        month = ts.format("MM")
        day = ts.format("DD")
        return f"{base}/{ts.year}/{ts.month:02d}/{ts.day:02d}.parquet"
        # return f"{base}/{year}/{month}/{day}.parquet"


def _find_existing_blobs(
        dates: list[pendulum.DateTime],
        fs_client,
        azure_path_env: str,
) -> dict[pendulum.DateTime, str]:
    results = {}
    for ts in dates:
        path = _blob_path_for_date(ts, azure_path_env)
        try:
            fs_client.get_file_client(path).get_file_properties()
            results[ts] = "success"
        except ResourceNotFoundError:
            results[ts] = "missing"
    return results


def _find_existing_postgres(
        dates: list[pendulum.DateTime],
        engine,
        postgres_table: str,
        postgres_date_col: str,
) -> dict[pendulum.DateTime, str]:
    results = {}

    if not dates:
        return results

    start = dates[0].date()
    end = dates[-1].date()

    query = text(f"""
        SELECT DISTINCT {postgres_date_col}::date AS d
        FROM {postgres_table}
        WHERE {postgres_date_col}::date BETWEEN :start AND :end
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"start": start, "end": end}).fetchall()

    found = {row[0] for row in rows}

    for ts in dates:
        results[ts] = "success" if ts.date() in found else "missing"

    return results


# ── state table helpers ───────────────────────────────────────────────────────

def get_last_reconciled_date(pipeline_name: str) -> pendulum.DateTime | None:
    """
    Returns the latest partition_date in processing_state for this pipeline.
    Used to determine reconcile start_date on subsequent runs.
    """
    conn = psycopg2.connect(config("DB_CONN_RAW"))
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(partition_date)
                FROM processing_state
                WHERE processing_level = %s
                  AND status != 'abandoned'
                """,
                (pipeline_name,),
            )
            row = cur.fetchone()
            if row and row[0]:
                return pendulum.datetime(row[0].year, row[0].month, row[0].day, tz="UTC")
            return None
    finally:
        conn.close()


def get_current_retry_count(processing_level: str, partition_date: pendulum.DateTime) -> int:
    """Reads the current retry_count for a single row."""
    conn = psycopg2.connect(config("DB_CONN_RAW"))
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT retry_count
                FROM processing_state
                WHERE processing_level = %s AND partition_date = %s
                """,
                (processing_level, partition_date.date()),
            )
            row = cur.fetchone()
            return row[0] if row else 0
    finally:
        conn.close()


def upsert_state_fn(
        processing_level: str,
        partition_date: pendulum.DateTime,
        status: str,
        expected_count: int = None,
        actual_count: int = None,
        error_type: str = None,
        error_message: str = None,
):
    """Idempotent upsert for processing_state."""
    conn = psycopg2.connect(config("DB_CONN_RAW"))

    query = """
        INSERT INTO processing_state (
            processing_level, partition_date, status,
            expected_count, actual_count,
            completeness_ratio, is_acceptable,
            updated_at, error_type, error_message
        )
        VALUES (
            %s, %s, %s, %s, %s,
            CASE WHEN %s IS NOT NULL AND %s IS NOT NULL
                 THEN %s::float / NULLIF(%s, 0) ELSE NULL END,
            CASE WHEN %s IS NOT NULL AND %s IS NOT NULL
                 THEN (%s::float / NULLIF(%s, 0)) >= 1 ELSE NULL END,
            NOW(), %s, %s
        )
        ON CONFLICT (processing_level, partition_date) DO UPDATE SET
            status             = EXCLUDED.status,
            expected_count     = EXCLUDED.expected_count,
            actual_count       = EXCLUDED.actual_count,
            completeness_ratio = EXCLUDED.completeness_ratio,
            is_acceptable      = EXCLUDED.is_acceptable,
            updated_at         = NOW(),
            error_type         = EXCLUDED.error_type,
            error_message      = EXCLUDED.error_message,
            retry_count        = CASE
                WHEN EXCLUDED.status = 'pending'
                THEN processing_state.retry_count + 1
                ELSE processing_state.retry_count
            END
        WHERE processing_state.status != 'abandoned';
    """

    params = (
        processing_level, partition_date.date(), status,
        expected_count, actual_count,
        actual_count, expected_count, actual_count, expected_count,
        actual_count, expected_count, actual_count, expected_count,
        error_type, error_message,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()
    finally:
        conn.close()


# ── universal reconcile ───────────────────────────────────────────────────────

def reconcile_processing_state(
        pipeline_name: str,
        start_date,
        end_date,
        fs_client=None,
        engine=None,
):
    """
    Universal reconciliation — runs on every flow start.
    Checks the pipeline's own OUTPUT destination (not source):
      gold_weekly → gold_weekly_summarized_data + weekly parquet
      gold_daily  → gold_daily_summarized_data  + daily parquet

    Status written:
      success → already processed, skip
      pending → not yet processed, get_pending_work will pick up

    Never overwrites 'abandoned' rows.
    """
    logger = get_logger()
    cfg = PIPELINE_CONFIG[pipeline_name]

    expected_dates = generate_dates(start_date, end_date, grain=cfg["grain"])
    logger.info(f"[reconcile] {pipeline_name}: {len(expected_dates)} partitions to check")

    azure_map, postgres_map = {}, {}

    if "azure" in cfg["source_check"] and fs_client:
        azure_map = _find_existing_blobs(
            expected_dates,
            fs_client,
            cfg["azure_path_env"],
        )

    if "postgres" in cfg["source_check"] and engine:
        postgres_map = _find_existing_postgres(
            expected_dates,
            engine,
            cfg["postgres_table"],
            cfg["postgres_date_col"],
        )

    for d in expected_dates:
        a = azure_map.get(d, "missing")
        p = postgres_map.get(d, "missing")

        if a == "success" or p == "success":
            status = "success"
            actual_count = cfg["expected_count"]
        else:
            status = "pending"
            actual_count = 0

        expected_count = cfg.get("expected_count") or (
            d.days_in_month if cfg["grain"] == "month" else None
        )

        upsert_state_fn(
            processing_level=pipeline_name,
            partition_date=d,
            status=status,
            expected_count=expected_count,
            actual_count=actual_count,
        )

    logger.info(f"[reconcile] {pipeline_name}: done")