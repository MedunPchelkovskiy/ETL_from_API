import pendulum
import prefect
from decouple import config
from prefect import flow
from prefect.states import Completed
from sqlalchemy import create_engine

from src.clients.datalake_client import fs_client
from src.helpers.gold.extract import get_quarter, expected_months_map, critical_month_map
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.find_process_state import get_pending_work
from src.helpers.observability_helpers.pipeline_config import PIPELINE_CONFIG, PIPELINE_STATUS_MAP, PIPELINE_ERROR_MAP
from src.helpers.observability_helpers.state_helpers import reconcile_processing_state, get_last_reconciled_date, \
    upsert_state_fn, get_current_retry_count
from src.tasks.gold.extract_from_gold import get_monthly_gold_azure, get_monthly_gold_postgres

PIPELINE_NAME = "gold_yearly"     #TODO: Add season to build processing_level name in processing state table

@flow(name="Aggregate monthly to quarterly flow")
def monthly_to_quarterly_aggregation():
    logger = get_logger()
    now = pendulum.now("UTC")
    engine = create_engine(config("DB_CONN_RAW"))
    cfg = PIPELINE_CONFIG[PIPELINE_NAME]
    max_missing_ratio = cfg["max_missing_ratio"]

    # ── early gate ────────────────────────────────────────────────────────────
    """
        in case of manual triggering, backfills, ad-hoc reruns, multiple schedules,
        someone changing the deployment schedule accidentally, CI/CD redeployments with immediate execution
    """

    if now.month not in [1, 4, 7, 10]:
        logger.info("Not a quarterly month — skipping")
        return Completed(message="Skipped-NonQuarter")

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

    grouped = {}
    for month in pending_months:
        quarter = get_quarter(month)
        grouped.setdefault(quarter, []).append(month)

    logger.info(f"[{PIPELINE_NAME}] {len(pending_months)} month(s) to process")

    # ── extract ───────────────────────────────────────────────────────────────
    all_quarters_dfs = {}
    missing_months = []

    for quarter_id, quarter_months in grouped.items():
        expected = expected_months_map[quarter_id]
        missing = set(expected) - set(quarter_months)

        if critical_month_map[quarter_id] in missing or len(missing) > 1:
            missing_months.append(grouped[quarter_id])

            continue

        upsert_state_fn(
            processing_level=PIPELINE_NAME,
            partition_date=quarter_months[0],
            status="processing",
            expected_count=len(expected),
        )
        for month in quarter_months:
            month_label = month.to_date_string()
            try:
                month_df = get_monthly_gold_azure(month)
                all_quarters_dfs.setdefault(quarter_id, []).append((month, month_df))
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
                    all_quarters_dfs.setdefault(quarter_id, []).append((month, month_df))
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

        # ── missing months gate ─────────────────────────────────────────────────
        max_missing = 1

        if len(missing_months) > max_missing:
            current_retries = get_current_retry_count(PIPELINE_NAME, month_start)

            if current_retries >= cfg["max_retries"]:
                status = "abandoned"
                error_message = (
                    f"Abandoned after {current_retries} retries — "
                    f"no source data available. Missing days: {missing_days}"  # ← days не weeks
                )
                logger.error(
                    f"[{PIPELINE_NAME}] Month {month_label} abandoned after "
                    f"{current_retries} retries — manual review required"
                )
            else:
                status = "pending"
                error_message = f"Missing days: {missing_days}"  # ← days не weeks
                logger.warning(
                    f"[{PIPELINE_NAME}] Month {month_label} — "
                    f"{len(missing_days)}/{month_start.days_in_month} missing days "  # ← не /4
                    f"| retry {current_retries + 1}/{cfg['max_retries']}"
                )

            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=month_start,
                status=status,
                expected_count=month_start.days_in_month,  # ← динамично, не cfg
                actual_count=month_start.days_in_month - len(missing_days),  # ← не 7 -
                error_type="missing_partitions",
                error_message=error_message,
            )
            skipped_months.append(month_label)
            continue

        if missing_days:
            logger.warning(
                f"[{PIPELINE_NAME}] Month {month_label} — proceeding with "
                f"{len(missing_days)} missing day(s): {missing_days}"  # ← days не weeks
            )
