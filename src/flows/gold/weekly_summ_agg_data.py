from typing import Any

import pandas as pd
import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from prefect import flow
from prefect.states import Completed

from src.helpers.gold.extract import get_last_processed_timestamp
from src.helpers.gold.load import update_last_processed_timestamp
from src.helpers.gold.pendulum_date_parser import ensure_pendulum
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.tasks.gold.extract_from_gold import get_daily_gold_azure, get_daily_gold_postgres
from src.tasks.gold.load_gold_data import load_gold_weekly_summ_data_to_azure, load_gold_weekly_summ_data_to_postgres
from src.tasks.gold.transform_gold_data import get_weekly_summ_data


@flow(name="Aggregate daily to weekly flow")
def daily_to_weekly_aggregation(week_start: Any = None):
    logger = get_logger()

    week_start = ensure_pendulum(week_start)
    # # coerce whatever Prefect passes in
    # if isinstance(week_start, str):
    #     week_start = pendulum.parse(week_start)
    # elif isinstance(week_start,     datetime):  # plain datetime from scheduler
    #     week_start = pendulum.instance(week_start)

    now = pendulum.now("UTC")
    pipeline_name = "Aggregate daily to weekly flow"

    logger.info(
        f"Starting {pipeline_name}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": now.to_iso8601_string(),
        }
    )

    # ── determine starting week ───────────────────────────────────
    last_processed = get_last_processed_timestamp(pipeline_name=pipeline_name)

    if week_start is None:
        if last_processed is None:
            current_week_start = now.start_of("week").subtract(weeks=1)
            logger.info("First run detected. Starting from last week.")
        else:
            current_week_start = last_processed.add(weeks=1).start_of("week")
    else:
        current_week_start = week_start.start_of('week')

    last_full_week_start = now.start_of("week").subtract(weeks=1)

    # ── early exit ────────────────────────────────────────────────
    if current_week_start > last_full_week_start:
        logger.info(
            f"No unprocessed weeks. "
            f"Last processed: {last_processed.to_date_string() if last_processed else 'None'}. "
            f"Skipping."
        )
        return Completed(message="Skipped-AlreadyProcessed")

    # ── catchup loop ──────────────────────────────────────────────
    all_weeks: dict[pendulum.DateTime, list[tuple[pendulum.DateTime, pd.DataFrame]]] = {}
    problematic_weeks: list[str] = []

    while current_week_start <= last_full_week_start:
        week_end = current_week_start.end_of("week")
        logger.info(f"Processing week: {current_week_start.to_date_string()} → {week_end.to_date_string()}")

        try:
            all_days_dfs, missing_days = get_daily_gold_azure(current_week_start, week_end)
        except ResourceNotFoundError as e:
            logger.info(
                f"No parquet file found for day  {current_week_start}.parquet, fall back to postgres | error={e}",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id
                })
            all_days_dfs, missing_days = get_daily_gold_postgres(current_week_start, week_end)

        # ── missing days gate ─────────────────────────────────────
        if len(missing_days) > 3:
            logger.warning(
                f"Week {current_week_start.to_date_string()} has too many missing days "
                f"({len(missing_days)}/7): {missing_days}. "
                f"Skipping week, continuing catchup."
            )
            problematic_weeks.append(current_week_start.to_date_string())
            current_week_start = current_week_start.add(weeks=1)
            continue  # no metadata update — will retry next run

        if missing_days:
            logger.warning(
                f"Week {current_week_start.to_date_string()} — "
                f"proceeding with {len(missing_days)} missing day(s): {missing_days}"
            )

        if all_days_dfs:
            all_weeks[current_week_start] = all_days_dfs
            logger.info(
                f"Stored week {current_week_start.to_date_string()} — "
                f"{len(all_days_dfs)}/7 days fetched"
            )
        else:
            logger.warning(f"No data at all for week {current_week_start.to_date_string()}, skipping.")

        update_last_processed_timestamp(pipeline_name,
                                        current_week_start)  # TODO: update time stamp after success processing, instead of just fetching!!!
        logger.info(
            f"Week {current_week_start.to_date_string()} marked as processed.")  # TODO: update time stamp after success processing, instead of just fetching!!!

        current_week_start = current_week_start.add(weeks=1)

    # ── report problematic weeks ──────────────────────────────────
    if problematic_weeks:
        logger.warning(
            f"Catchup finished with {len(problematic_weeks)} skipped week(s) "
            f"due to missing data: {problematic_weeks}. Manual review required."
        )

    # ── downstream processing per week ────────────────────────────
    if not all_weeks:
        logger.warning("No weeks collected for downstream processing.")
        return Completed(message="Skipped-NoNewData")

    futures = []

    for wk_start, days in all_weeks.items():
        futures.append(get_weekly_summ_data.submit(wk_start, days))
    all_weeks_summ = []

    for f in futures:
        try:
            all_weeks_summ.append(f.result())
        except Exception as e:
            logger.warning(f"Skipping failed week: {e}")

    # ── downstream loading per week ────────────────────────────

    for week in all_weeks_summ:
        load_gold_weekly_summ_data_to_azure(pipeline_name, week)
    load_gold_weekly_summ_data_to_postgres(all_weeks_summ)

    # # print first 5 rows vertically for dev logs
    # for i, (ts, df) in enumerate(daily_summ_result[:5]):
    #     logger.info("\nItem %s timestamp: %s", i, ts)
    #     logger.info(
    #         "\nItem %s dataframe (first row vertical):\n%s",
    #         i,
    #         df.iloc[0].to_frame().to_string() if not df.empty else "Empty DataFrame"
    #     )

    logger.info(
        f"Finished {pipeline_name}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": pendulum.now("UTC").to_iso8601_string(),
        }
    )


if __name__ == "__main__":
    # now = pendulum.now("UTC")
    # daily_to_weekly_aggregation(now.start_of("week").subtract(weeks=1))
    daily_to_weekly_aggregation()
