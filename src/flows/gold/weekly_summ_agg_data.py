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

        # ── extract: Azure first, Postgres fallback ───────────────────────────
        try:
            all_days_dfs, missing_days = get_daily_gold_azure(week_dates)
        except Exception as e:
            logger.warning(
                f"[{PIPELINE_NAME}] Azure failed for {week_label}, falling back to Postgres | {e}"
            )
            try:
                all_days_dfs, missing_days = get_daily_gold_postgres(week_dates)
            except Exception as e2:
                logger.error(
                    f"[{PIPELINE_NAME}] Postgres fallback also failed for {week_label} | {e2}"
                )
                upsert_state_fn(
                    processing_level=PIPELINE_NAME,
                    partition_date=week_start,
                    status="failed",
                    expected_count=cfg["expected_count"],
                    actual_count=0,
                    error_type="missing_partitions",
                    error_message=str(e2),
                )
                failed_weeks.append(week_label)
                continue

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
                    f"[{PIPELINE_NAME}] Week {week_label} — {len(missing_days)}/7 missing days "
                    f"retry {current_retries + 1}/{cfg['max_retries']}"
                )

            upsert_state_fn(
                processing_level=PIPELINE_NAME,
                partition_date=week_start,
                status=status,
                expected_count=cfg["expected_count"],
                actual_count=7 - len(missing_days),
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

    if failed_weeks:
        raise RuntimeError(
            f"[{PIPELINE_NAME}] {len(failed_weeks)} week(s) failed: {failed_weeks}"
        )


if __name__ == "__main__":
    daily_to_weekly_aggregation()

# from typing import Any
#
# import pandas as pd
# import pendulum
# import prefect
# from azure.core.exceptions import ResourceNotFoundError
# from decouple import config
# from prefect import flow
# from prefect.states import Completed
# from sqlalchemy import create_engine
#
# from src.clients.datalake_client import fs_client
# from src.helpers.gold.pendulum_date_parser import ensure_pendulum
# from src.helpers.logging_helpers.combine_loggers_helper import get_logger
# from src.helpers.observability_helpers.find_process_state import get_pending_work
# from src.helpers.observability_helpers.initial_run_states import reconcile_processing_state
# from src.helpers.observability_helpers.status_error_mappers import PIPELINE_STATUS_MAP, PIPELINE_ERROR_MAP
# from src.tasks.gold.extract_from_gold import get_daily_gold_azure, get_daily_gold_postgres
# from src.tasks.gold.load_gold_data import load_gold_weekly_summ_data_to_azure, load_gold_weekly_summ_data_to_postgres
# from src.tasks.gold.transform_gold_data import get_weekly_summ_data
#
#
# @flow(name="Aggregate daily to weekly flow")
# def daily_to_weekly_aggregation(week_start: Any = None):
#     logger = get_logger()
#
#     week_start = ensure_pendulum(week_start)
#
#     now = pendulum.now("UTC")
#     pipeline_name = "Aggregate daily to weekly flow"
#     statuses = PIPELINE_STATUS_MAP["gold_weekly"]
#     errors = PIPELINE_ERROR_MAP["gold_weekly"]
#     engine = create_engine(config("DB_CONN_RAW"))
#     start_date = pendulum.datetime(now.year, 1, 4).start_of("week")
#     end_date = now.start_of("week").subtract(weeks=1)
#
#     logger.info(
#         f"Starting {pipeline_name}",
#         extra={
#             "flow_run_id": prefect.runtime.flow_run.id,
#             "utc_time": now.to_iso8601_string(),
#         }
#     )
#
#     # ── determine week for processing ───────────────────────────────────
#
#     days_to_process = get_pending_work(pipeline_name, [statuses], [errors])
#     if not days_to_process:
#         reconcile_processing_state(pipeline_name, start_date, end_date, fs_client, engine)
#
#     # last_processed = get_last_processed_timestamp(pipeline_name=pipeline_name)
#     #
#     # if week_start is None:
#     #     if last_processed is None:
#     #         current_week_start = now.start_of("week").subtract(weeks=1)
#     #         logger.info("First run detected. Starting from last week.")
#     #     else:
#     #         current_week_start = last_processed.add(weeks=1).start_of("week")
#     # else:
#     #     current_week_start = week_start.start_of('week')
#     #
#     # last_full_week_start = now.start_of("week").subtract(weeks=1)
#     #
#     # # ── early exit ────────────────────────────────────────────────
#     # if current_week_start > last_full_week_start:
#     #     logger.info(
#     #         f"No unprocessed weeks. "
#     #         f"Last processed: {last_processed.to_date_string() if last_processed else 'None'}. "
#     #         f"Skipping."
#     #     )
#     #     return Completed(message="Skipped-AlreadyProcessed")
#
#     # ── catchup loop ──────────────────────────────────────────────
#
#     all_weeks: dict[pendulum.DateTime, list[tuple[pendulum.DateTime, pd.DataFrame]]] = {}
#     problematic_weeks: list[str] = []
#
#     while current_week_start <= last_full_week_start:
#         week_end = current_week_start.end_of("week")
#         logger.info(f"Processing week: {current_week_start.to_date_string()} → {week_end.to_date_string()}")
#
#         try:
#             all_days_dfs, missing_days = get_daily_gold_azure(current_week_start, week_end)
#         except ResourceNotFoundError as e:
#             logger.info(
#                 f"No parquet file found for day  {current_week_start}.parquet, fall back to postgres | error={e}",
#                 extra={
#                     "flow_run_id": prefect.runtime.flow_run.id,
#                     "task_run_id": prefect.runtime.task_run.id
#                 })
#             all_days_dfs, missing_days = get_daily_gold_postgres(current_week_start, week_end)
#
#         # ── missing days gate ─────────────────────────────────────
#         if len(missing_days) > 3:
#             logger.warning(
#                 f"Week {current_week_start.to_date_string()} has too many missing days "
#                 f"({len(missing_days)}/7): {missing_days}. "
#                 f"Skipping week, continuing catchup."
#             )
#             problematic_weeks.append(current_week_start.to_date_string())
#             current_week_start = current_week_start.add(weeks=1)
#             continue  # no metadata update — will retry next run
#
#         if missing_days:
#             logger.warning(
#                 f"Week {current_week_start.to_date_string()} — "
#                 f"proceeding with {len(missing_days)} missing day(s): {missing_days}"
#             )
#
#         if all_days_dfs:
#             all_weeks[current_week_start] = all_days_dfs
#             logger.info(
#                 f"Stored week {current_week_start.to_date_string()} — "
#                 f"{len(all_days_dfs)}/7 days fetched"
#             )
#         else:
#             logger.warning(f"No data at all for week {current_week_start.to_date_string()}, skipping.")
#
#         # update_last_processed_timestamp(pipeline_name,
#         #                                 current_week_start)  # TODO: update time stamp after success processing, instead of just fetching!!!
#         # logger.info(
#         #     f"Week {current_week_start.to_date_string()} marked as processed.")  # TODO: update time stamp after success processing, instead of just fetching!!!
#
#         current_week_start = current_week_start.add(weeks=1)
#
#     # ── report problematic weeks ──────────────────────────────────
#     if problematic_weeks:
#         logger.warning(
#             f"Catchup finished with {len(problematic_weeks)} skipped week(s) "
#             f"due to missing data: {problematic_weeks}. Manual review required."
#         )
#
#     # ── downstream processing per week ────────────────────────────
#     if not all_weeks:
#         logger.warning("No weeks collected for downstream processing.")
#         return Completed(message="Skipped-NoNewData")
#
#     futures = []
#
#     for wk_start, days in all_weeks.items():
#         futures.append(get_weekly_summ_data.submit(wk_start, days))
#     all_weeks_summ = []
#
#     for f in futures:
#         try:
#             all_weeks_summ.append(f.result())
#         except Exception as e:
#             logger.warning(f"Skipping failed week: {e}")
#
#     # ── downstream loading per week ────────────────────────────
#
#     errors = []
#
#     for week in all_weeks_summ:
#         try:
#             load_gold_weekly_summ_data_to_azure(pipeline_name, week)
#         except Exception as e:
#             logger.exception("Azure upload failed for week=%s year=%s",
#                              week["week_number"].iloc[0], week["year"].iloc[0])
#             errors.append(("azure", week["week_number"].iloc[0], e))
#
#     try:
#         load_gold_weekly_summ_data_to_postgres(pipeline_name, all_weeks_summ)
#     except Exception as e:
#         logger.exception("Postgres load failed")
#         errors.append(("postgres", None, e))
#
#     if errors:
#         raise RuntimeError(f"Load completed with {len(errors)} error(s): {errors}")
#
#     logger.info(
#         f"Finished {pipeline_name}",
#         extra={
#             "flow_run_id": prefect.runtime.flow_run.id,
#             "utc_time": pendulum.now("UTC").to_iso8601_string(),
#         }
#     )
#
#
# if __name__ == "__main__":
#     # now = pendulum.now("UTC")
#     # daily_to_weekly_aggregation(now.start_of("week").subtract(weeks=1))
#     daily_to_weekly_aggregation()
