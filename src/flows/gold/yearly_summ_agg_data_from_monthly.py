import pandas as pd
import pendulum
import prefect
from decouple import config
from prefect import flow
from prefect.states import Completed
from sqlalchemy import create_engine

from src.clients.datalake_client import fs_client
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.find_process_state import get_pending_work
from src.helpers.observability_helpers.initial_run_states import generate_dates
from src.helpers.observability_helpers.pipeline_config import PIPELINE_CONFIG, PIPELINE_STATUS_MAP, PIPELINE_ERROR_MAP
from src.helpers.observability_helpers.state_helpers import get_last_reconciled_date, reconcile_processing_state, \
    upsert_state_fn, get_current_retry_count
from src.tasks.gold.extract_from_gold import get_monthly_gold_azure
from src.tasks.gold.transform_gold_data import aggregate_gold_months

PIPELINE_NAME = "gold_yearly"

@flow(name="Aggregate monthly to yearly flow")
def monthly_to_yearly_aggregation():
    logger = get_logger()
    now = pendulum.now("UTC")
    engine = create_engine(config("DB_CONN_RAW"))
    cfg = PIPELINE_CONFIG[PIPELINE_NAME]
    max_missing_ratio = cfg["max_missing_ratio"]

    # ── early gate ────────────────────────────────────────────────────────────
    if now.month < 3:
        logger.info(f"[{PIPELINE_NAME}] Too early — minimum 2 months required")
        return Completed(message="Skipped-TooEarly")

    logger.info(
        f"Starting {PIPELINE_NAME}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": now.to_iso8601_string(),
        },
    )

    end_date = now.start_of("month")  # текущият месец excluded
    expected_months = now.month - 1
    max_missing = round(expected_months * max_missing_ratio)

    last_reconciled = get_last_reconciled_date(PIPELINE_NAME)

    if last_reconciled is None:
        reconcile_start = pendulum.datetime(now.year, 1, 1)
        logger.info(f"[{PIPELINE_NAME}] First run — reconciling from {reconcile_start.to_date_string()}")
    else:
        reconcile_start = last_reconciled
        logger.info(
            f"[{PIPELINE_NAME}] Reconciling from {reconcile_start.to_date_string()} "
            f"→ {end_date.to_date_string()}"
        )

    reconcile_processing_state(
        pipeline_name=PIPELINE_NAME,
        start_date=reconcile_start,
        end_date=end_date,
        fs_client=fs_client,
        engine=engine,
    )

    # ── fetch pending work ────────────────────────────────────────────────────
    pending_months: list[pendulum.DateTime] = get_pending_work(
        processing_level=PIPELINE_NAME,
        statuses=PIPELINE_STATUS_MAP[PIPELINE_NAME],
        error_types=PIPELINE_ERROR_MAP[PIPELINE_NAME],
        max_retries=cfg["max_retries"],
    )

    if not pending_months:
        logger.info(f"[{PIPELINE_NAME}] No pending months — nothing to do")
        return Completed(message="Skipped-AlreadyProcessed")

    logger.info(f"[{PIPELINE_NAME}] {len(pending_months)} month(s) to process")

    # ── extract ───────────────────────────────────────────────────────────────
    all_months_dfs = []
    missing_months = []

    for month in pending_months:
        month_label = month.to_date_string()

        upsert_state_fn(
            processing_level=PIPELINE_NAME,
            partition_date=month,
            status="processing",
            expected_count=expected_months,
        )

        try:
            month_df = get_monthly_gold_azure(month)
            all_months_dfs.append((month, month_df))
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=month,
                status="success",
                expected_count=expected_months,
            )
        except Exception as e:
            logger.warning(
                f"[{PIPELINE_NAME}] Azure failed for {month_label}, falling back to Postgres | {e}"
            )
            try:
                month_df = get_monthly_gold_postgres(month)
                all_months_dfs.append((month, month_df))
                upsert_state_fn(
                    processing_level=PIPELINE_NAME,
                    partition_date=month,
                    status="success",
                    expected_count=expected_months,
                )
            except Exception as e2:
                logger.error(
                    f"[{PIPELINE_NAME}] Postgres fallback also failed for {month_label} | {e2}"
                )
                missing_months.append(month)
                upsert_state_fn(
                    processing_level=PIPELINE_NAME,
                    partition_date=month,
                    status="pending",
                    expected_count=expected_months,
                )
                continue

    # ── missing months gate ───────────────────────────────────────────────────
    if len(missing_months) > max_missing:
        current_retries = get_current_retry_count(PIPELINE_NAME, pendulum.datetime(now.year, 1, 1))

        if current_retries >= cfg["max_retries"] and expected_months == 12:
            status = "abandoned"
            error_message = (
                f"Abandoned after {current_retries} retries — "
                f"no source data available. Missing months: {[m.to_date_string() for m in missing_months]}"
            )
            logger.error(f"[{PIPELINE_NAME}] Year {now.year} abandoned after {current_retries} retries")
        else:
            status = "pending"
            error_message = f"Missing months: {[m.to_date_string() for m in missing_months]}"
            logger.warning(
                f"[{PIPELINE_NAME}] Year {now.year} — "
                f"{len(missing_months)}/{expected_months} missing month(s) "
                f"| retry {current_retries + 1}/{cfg['max_retries']}"
            )

        upsert_state_fn(
            processing_level=PIPELINE_NAME,
            partition_date=pendulum.datetime(now.year, 1, 1),
            status=status,
            expected_count=expected_months,
            actual_count=expected_months - len(missing_months),
            error_type="missing_partitions",
            error_message=error_message,
        )
        return Completed(message="Skipped-InsufficientData")

    if missing_months:
        logger.warning(
            f"[{PIPELINE_NAME}] Year {now.year} — proceeding with "
            f"{len(missing_months)} missing month(s): {[m.to_date_string() for m in missing_months]}"
        )

    # ── transform ─────────────────────────────────────────────────────────

    try:
        year = now.year
        yearly_summ = aggregate_gold_months(all_months_dfs, expected_months, max_missing_ratio, year)
    except ValueError as e:
        upsert_state_fn(
            processing_level=PIPELINE_NAME,
            partition_date=pendulum.datetime(year, 1, 1),
            status="failed",
            expected_count=expected_months,
            actual_count=len(all_months_dfs),
            error_type="insufficient_data",
            error_message=str(e),
        )
        return
    except Exception as e:
        upsert_state_fn(
            processing_level=PIPELINE_NAME,
            partition_date=pendulum.datetime(year, 1, 1),
            status="failed",
            expected_count=expected_months,
            actual_count=len(all_months_dfs),
            error_type="transformation_error",
            error_message=str(e),
        )
        return



