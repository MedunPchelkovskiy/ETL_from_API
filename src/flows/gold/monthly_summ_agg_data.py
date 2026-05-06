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
from src.tasks.gold.extract_from_gold import get_daily_gold_azure, get_daily_gold_postgres
from src.tasks.gold.load_gold_data import load_gold_monthly_summ_data_to_azure, load_gold_monthly_summ_data_to_postgres
from src.tasks.gold.transform_gold_data import get_monthly_summ_data

PIPELINE_NAME = "gold_monthly"

@flow(name="Aggregate daily to monthly flow")
def daily_to_monthly_aggregation():
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
        reconcile_start = pendulum.datetime(now.year - 1, 1, 1)
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
        month_days = generate_dates(month_start, month_start.add(months=1), grain="day")
        expected_days = month_start.days_in_month

        # mark as processing — prevents duplicate runs
        upsert_state_fn(
            processing_level=PIPELINE_NAME,
            partition_date=month_start,
            status="processing",
            expected_count=expected_days,
        )
        # ── extract: Azure first, Postgres fallback ───────────────────────────
        try:
            all_days_dfs, missing_days = get_daily_gold_azure(month_days)
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
                    partition_date=month_start,
                    status="failed",
                    expected_count=expected_days,
                    actual_count=0,
                    error_type="missing_partitions",
                    error_message=str(e2),
                )
                failed_months.append(month_label)
                continue

        # ── missing days gate ─────────────────────────────────────────────────
        max_missing = round(month_start.days_in_month * max_missing_ratio)  # динамично

        if len(missing_days) > max_missing:
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
        # ── transform ─────────────────────────────────────────────────────────
        try:
            monthly_summ = get_monthly_summ_data(month_start, all_days_dfs, max_missing_ratio)
        except ValueError as e:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=month_start,
                status="failed",
                expected_count=expected_days,
                actual_count=len(all_days_dfs),
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
                expected_count=expected_days,
                actual_count=len(all_days_dfs),
                error_type="transformation_error",
                error_message=str(e),
            )
            failed_months.append(month_label)
            continue

        all_months_summ.append((month_start, monthly_summ))

    # ── load ──────────────────────────────────────────────────────────────────
    for month_start, monthly_summ in all_months_summ:
        month_label = month_start.to_date_string()
        expected_days = month_start.days_in_month
        azure_ok = False
        postgres_ok = False

        try:
            load_gold_monthly_summ_data_to_azure(PIPELINE_NAME, month_start, monthly_summ)
            azure_ok = True
        except Exception:
            logger.exception(f"[{PIPELINE_NAME}] Azure upload failed for {month_label}")

        try:
            load_gold_monthly_summ_data_to_postgres(PIPELINE_NAME, month_start,[monthly_summ])
            postgres_ok = True
        except Exception:
            logger.exception(f"[{PIPELINE_NAME}] Postgres load failed for {month_label}")

        if azure_ok or postgres_ok:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=month_start,
                status="success",
                expected_count=expected_days,
                actual_count=expected_days,
            )
        else:
            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=month_start,
                status="failed",
                expected_count=expected_days,
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
            f"[{PIPELINE_NAME}] {len(failed_months)} month(s) failed: {failed_months}"
        )

if __name__ == "__main__":
    daily_to_monthly_aggregation()
