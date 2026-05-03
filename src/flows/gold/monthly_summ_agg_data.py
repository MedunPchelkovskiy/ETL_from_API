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

PIPELINE_NAME = "gold_monthly"
MAX_MISSING_WEEKS = 1

@flow(name="Aggregate weekly to monthly flow")
def weekly_to_monthly_aggregation():
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

    year_start = pendulum.datetime(now.year, 1, 4).start_of("month")
    end_date = now.start_of("month")  # current month excluded to be sure processing full month (<, not <=)

    # ── reconcile: always run, only for new partitions ────────────────────────
    last_reconciled = get_last_reconciled_date(PIPELINE_NAME)

    if last_reconciled is None:
        # first run — reconcile from start of year
        reconcile_start = year_start
        logger.info(f"[{PIPELINE_NAME}] First run — reconciling from {year_start.to_date_string()}")
    else:
        # subsequent runs — reconcile only new months since last known partition
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
    )

    if not pending_months:
        logger.info(f"[{PIPELINE_NAME}] No pending months — nothing to do")
        return Completed(message="Skipped-AlreadyProcessed")

    logger.info(f"[{PIPELINE_NAME}] {len(pending_months)} month(s) to process")

    # ── process each pending month ─────────────────────────────────────────────
    all_months_summ = []
    skipped_months = []
    failed_months = []

    for month_start in pending_months:
        month_label = month_start.to_date_string()
        month_weeks = generate_dates(month_start, month_start.add(weeks=1), grain="week")

        # mark as processing — prevents duplicate runs
        upsert_state_fn(
            processing_level=PIPELINE_NAME,
            partition_date=month_start,
            status="processing",
            expected_count=cfg["expected_count"],
        )
        # ── extract: Azure first, Postgres fallback ───────────────────────────
        try:
            all_months_dfs, missing_weeks = get_weekly_gold_azure(month_weeks)
        except Exception as e:
            logger.warning(
                f"[{PIPELINE_NAME}] Azure failed for {month_label}, falling back to Postgres | {e}"
            )
            try:
                all_months_dfs, missing_weeks = get_weekly_gold_postgres(month_weeks)
            except Exception as e2:
                logger.error(
                    f"[{PIPELINE_NAME}] Postgres fallback also failed for {month_label} | {e2}"
                )
                upsert_state_fn(
                    processing_level=PIPELINE_NAME,
                    partition_date=month_start,
                    status="failed",
                    expected_count=cfg["expected_count"],
                    actual_count=0,
                    error_type="missing_partitions",
                    error_message=str(e2),
                )
                failed_months.append(month_label)
                continue

        # ── missing weeks gate ─────────────────────────────────────────────────
        if len(missing_weeks) > MAX_MISSING_WEEKS:
            current_retries = get_current_retry_count(PIPELINE_NAME, month_start)

            if current_retries >= cfg["max_retries"]:
                status = "abandoned"
                error_message = (
                    f"Abandoned after {current_retries} retries — "
                    f"no source data available. Missing weeks: {missing_weeks}"
                )
                logger.error(
                    f"[{PIPELINE_NAME}] Month {month_label} abandoned after "
                    f"{current_retries} retries — manual review required"
                )
            else:
                status = "pending"
                error_message = f"Missing weeks: {missing_weeks}"
                logger.warning(
                    f"[{PIPELINE_NAME}] Month {month_label} — {len(missing_weeks)}/4 missing weeks "
                    f"retry {current_retries + 1}/{cfg['max_retries']}"
                )

            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=month_start,
                status=status,
                expected_count=cfg["expected_count"],
                actual_count=7 - len(missing_weeks),
                error_type="missing_partitions",
                error_message=error_message,
            )
            skipped_months.append(month_label)
            continue

        if missing_weeks:
            logger.warning(
                f"[{PIPELINE_NAME}] Month {month_label} — proceeding with "
                f"{len(missing_weeks)} missing weeks(s): {missing_weeks}"
            )

        # ── transform ─────────────────────────────────────────────────────────
        try:
            monthly_summ = get_monthly_summ_data(month_start, all_months_dfs)
        except ValueError as e:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=month_start,
                status="failed",
                expected_count=cfg["expected_count"],
                actual_count=len(all_months_dfs),
                error_type="insufficient_data",
                error_message=str(e),
            )
            failed_months.append(month_label)
            continue
        except Exception as e:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=month_start,
                status="failed",
                expected_count=cfg["expected_count"],
                actual_count=len(all_months_dfs),
                error_type="transformation_error",
                error_message=str(e),
            )
            failed_months.append(month_label)
            continue

        all_months_summ.append((month_start, monthly_summ))

    # ── load ──────────────────────────────────────────────────────────────────
    for month_start, monthly_summ in all_months_summ:
        month_label = month_start.to_date_string()
        azure_ok = False
        postgres_ok = False

        try:
            load_gold_monthly_summ_data_to_azure(PIPELINE_NAME, monthly_summ)
            azure_ok = True
        except Exception:
            logger.exception(f"[{PIPELINE_NAME}] Azure upload failed for {month_label}")

        try:
            load_gold_monthly_summ_data_to_postgres(PIPELINE_NAME, [monthly_summ])
            postgres_ok = True
        except Exception:
            logger.exception(f"[{PIPELINE_NAME}] Postgres load failed for {month_label}")

        if azure_ok or postgres_ok:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=month_start,
                status="success",
                expected_count=cfg["expected_count"],
                actual_count=cfg["expected_count"],
            )
        else:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=month_start,
                status="failed",
                expected_count=cfg["expected_count"],
                actual_count=0,
                error_type="missing_partitions",
                error_message="Both Azure and Postgres load failed",
            )
            failed_months.append(month_label)
    # ── final report ──────────────────────────────────────────────────────────
    logger.info(
        f"[{PIPELINE_NAME}] Finished | "
        f"success={len(all_months_summ)} "
        f"skipped={len(skipped_months)} "
        f"failed={len(failed_months)}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": pendulum.now("UTC").to_iso8601_string(),
        },
    )

    if skipped_months:
        logger.warning(f"[{PIPELINE_NAME}] Skipped (missing data): {skipped_months}")

    if failed_months:
        raise RuntimeError(
            f"[{PIPELINE_NAME}] {len(failed_months)} week(s) failed: {failed_months}"
        )

if __name__ == "__main__":
    weekly_to_monthly_aggregation()
