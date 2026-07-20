import pendulum
import prefect
from decouple import config
from prefect import flow
from prefect.states import Completed
from sqlalchemy import create_engine

from src.helpers.observability_helpers.decorators import measure_flow_duration
from src.clients.datalake_client import fs_client
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.find_process_state import get_pending_work
from src.helpers.observability_helpers.initial_run_states import generate_dates
from src.helpers.observability_helpers.pipeline_config import (
    PIPELINE_CONFIG,
    PIPELINE_STATUS_MAP,
    PIPELINE_ERROR_MAP,
)
from src.helpers.observability_helpers.state_helpers import (
    get_last_reconciled_date,
    get_current_retry_count,
    reconcile_processing_state,
    upsert_state_fn,
)
from src.tasks.gold.extract_from_gold import (
    get_daily_gold_azure, get_daily_gold_postgres,
)
from src.tasks.gold.load_gold_data import (
    load_gold_weekly_summ_data_to_azure,
    load_gold_weekly_summ_data_to_postgres,
)
from src.tasks.gold.transform_gold_data import get_weekly_summ_data

PIPELINE_NAME = "gold_weekly"
MAX_MISSING_DAYS = 3


# ── flow ──────────────────────────────────────────────────────────────────────

@flow(name="Aggregate daily to weekly flow")
@measure_flow_duration(flow_name="gold_weekly_summ_flow")
def daily_to_weekly_aggregation():
    logger = get_logger()
    now = pendulum.now("UTC")
    engine = create_engine(config("DB_CONN_RAW"))
    cfg = PIPELINE_CONFIG[PIPELINE_NAME]

    logger.info(
        f"Starting {PIPELINE_NAME}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": now.to_iso8601_string(),
        },
    )

    # ── date boundaries ───────────────────────────────────────────────────────
    year_start = pendulum.datetime(now.year, 1, 4).start_of("week")
    end_date = now.start_of("week")  # current week excluded (<, not <=)

    # ── reconcile: always run, only for new partitions ────────────────────────
    last_reconciled = get_last_reconciled_date(PIPELINE_NAME)

    if last_reconciled is None:
        # first run — reconcile from start of year
        reconcile_start = year_start
        logger.info(f"[{PIPELINE_NAME}] First run — reconciling from {year_start.to_date_string()}")
    else:
        # subsequent runs — reconcile only new weeks since last known partition
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
    pending_weeks: list[pendulum.DateTime] = get_pending_work(
        processing_level=PIPELINE_NAME,
        statuses=PIPELINE_STATUS_MAP[PIPELINE_NAME],
        error_types=PIPELINE_ERROR_MAP[PIPELINE_NAME],
        max_retries=cfg["max_retries"],
    )

    if not pending_weeks:
        logger.info(f"[{PIPELINE_NAME}] No pending weeks — nothing to do")
        return Completed(message="Skipped-AlreadyProcessed")

    logger.info(f"[{PIPELINE_NAME}] {len(pending_weeks)} week(s) to process")

    # ── process each pending week ─────────────────────────────────────────────
    all_weeks_summ = []
    skipped_weeks = []
    failed_weeks = []

    for week_start in pending_weeks:
        week_label = week_start.to_date_string()
        week_dates = generate_dates(week_start, week_start.add(weeks=1), grain="day")

        # mark as processing — prevents duplicate runs
        upsert_state_fn(
            processing_level=PIPELINE_NAME,
            partition_date=week_start,
            status="processing",
            expected_count=cfg["expected_count"],
        )

        # ── extract: Azure first, Postgres fallback за missing дни ────────────
        all_days_dfs, missing_days = get_daily_gold_azure(week_dates, pipeline_name=PIPELINE_NAME)

        if missing_days:
            missing_dates = [d for d in week_dates if d.to_date_string() in missing_days]
            logger.info(f"[{PIPELINE_NAME}] {len(missing_dates)} day(s) missing from Azure, trying Postgres")
            pg_dfs, still_missing = get_daily_gold_postgres(missing_dates, engine, pipeline_name=PIPELINE_NAME)
            all_days_dfs.extend(pg_dfs)
            missing_days = still_missing

        # ── missing days gate ─────────────────────────────────────────────────
        if len(missing_days) > MAX_MISSING_DAYS:
            current_retries = get_current_retry_count(PIPELINE_NAME, week_start)

            if current_retries >= cfg["max_retries"]:
                status = "abandoned"
                error_message = (
                    f"Abandoned after {current_retries} retries — "
                    f"no source data available. Missing days: {missing_days}"
                )
                logger.error(
                    f"[{PIPELINE_NAME}] Week {week_label} abandoned after "
                    f"{current_retries} retries — manual review required"
                )
            else:
                status = "pending"
                error_message = f"Missing days: {missing_days}"
                logger.warning(
                    f"[{PIPELINE_NAME}] Week {week_label} — {len(missing_days)}/{cfg['expected_count']} missing days "
                    f"retry {current_retries + 1}/{cfg['max_retries']}"
                )

            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=week_start,
                status=status,
                expected_count=cfg["expected_count"],
                actual_count=cfg["expected_count"] - len(missing_days),
                error_type="missing_partitions",
                error_message=error_message,
            )
            skipped_weeks.append(week_label)
            continue

        if missing_days:
            logger.warning(
                f"[{PIPELINE_NAME}] Week {week_label} — proceeding with "
                f"{len(missing_days)} missing day(s): {missing_days}"
            )

        # ── transform ─────────────────────────────────────────────────────────
        try:
            weekly_summ = get_weekly_summ_data(week_start, all_days_dfs)
        except ValueError as e:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=week_start,
                status="failed",
                expected_count=cfg["expected_count"],
                actual_count=len(all_days_dfs),
                error_type="insufficient_data",
                error_message=str(e),
            )
            failed_weeks.append(week_label)
            continue
        except Exception as e:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=week_start,
                status="failed",
                expected_count=cfg["expected_count"],
                actual_count=len(all_days_dfs),
                error_type="transformation_error",
                error_message=str(e),
            )
            failed_weeks.append(week_label)
            continue

        all_weeks_summ.append((week_start, weekly_summ))

    # ── load ──────────────────────────────────────────────────────────────────
    for week_start, weekly_summ in all_weeks_summ:
        week_label = week_start.to_date_string()
        azure_ok = False
        postgres_ok = False

        try:
            load_gold_weekly_summ_data_to_azure(PIPELINE_NAME, weekly_summ)
            azure_ok = True
        except Exception:
            logger.exception(f"[{PIPELINE_NAME}] Azure upload failed for {week_label}")

        try:
            load_gold_weekly_summ_data_to_postgres(PIPELINE_NAME, [weekly_summ])
            postgres_ok = True
        except Exception:
            logger.exception(f"[{PIPELINE_NAME}] Postgres load failed for {week_label}")

        if azure_ok or postgres_ok:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=week_start,
                status="success",
                expected_count=cfg["expected_count"],
                actual_count=cfg["expected_count"],
            )
        else:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=week_start,
                status="failed",
                expected_count=cfg["expected_count"],
                actual_count=0,
                error_type="missing_partitions",
                error_message="Both Azure and Postgres load failed",
            )
            failed_weeks.append(week_label)

    # ── final report ──────────────────────────────────────────────────────────
    logger.info(
        f"[{PIPELINE_NAME}] Finished | "
        f"success={len(all_weeks_summ)} "
        f"skipped={len(skipped_weeks)} "
        f"failed={len(failed_weeks)}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": pendulum.now("UTC").to_iso8601_string(),
        },
    )

    if skipped_weeks:
        logger.warning(f"[{PIPELINE_NAME}] Skipped (missing data): {skipped_weeks}")

    all_failed = skipped_weeks + failed_weeks
    if all_failed:
        return Completed(message=f"Partial-Failure: {len(all_failed)} week(s) failed: {all_failed}")

    return Completed(message=f"Success: {len(all_weeks_summ)} week(s) processed")


if __name__ == "__main__":
    daily_to_weekly_aggregation()
