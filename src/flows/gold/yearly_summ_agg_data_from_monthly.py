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
    upsert_state_fn
from src.tasks.gold.extract_from_gold import get_monthly_gold_azure

PIPELINE_NAME = "gold_yearly"


@flow(
    name="Aggregate monthly to yearly flow",
)
def monthly_to_yearly_aggregation():
    logger = get_logger()
    now = pendulum.now("UTC")
    engine = create_engine(config("DB_CONN_RAW"))
    cfg = PIPELINE_CONFIG[PIPELINE_NAME]
    max_missing_ratio = cfg["max_missing_ratio"]

    logger.info(
        f"Starting {PIPELINE_NAME}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": now.to_iso8601_string(),
        },
    )

    now = pendulum.now("UTC")
    end_date = now.start_of("month")  # текущият месец excluded

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

    # ── process each pending month ─────────────────────────────────────────────
    all_months_summ = []
    skipped_months = []
    failed_months = []

    for month in pending_months:
        month_label = month.to_date_string()
        expected_days = month.days_in_month

        # mark as processing — prevents duplicate runs
        upsert_state_fn(
            processing_level=PIPELINE_NAME,
            partition_date=month,
            status="processing",
            expected_count=expected_days,
        )
        # ── extract: Azure first, Postgres fallback ───────────────────────────
        try:
            month_df= get_monthly_gold_azure(month)
        except Exception as e:
            logger.warning(
                f"[{PIPELINE_NAME}] Azure failed for {month_label}, falling back to Postgres | {e}"
            )
            try:
                all_days_dfs, missing_days = get_daily_gold_postgres(month_days)
            except Exception as e2:
                logger.error(
                    f"[{PIPELINE_NAME}] Postgres fallback also failed for {month_label} | {e2}"
                )
                upsert_state_fn(
                    processing_level=PIPELINE_NAME,
                    partition_date=month,
                    status="failed",
                    expected_count=expected_days,
                    actual_count=0,
                    error_type="missing_partitions",
                    error_message=str(e2),
                )
                failed_months.append(month_label)
                continue

