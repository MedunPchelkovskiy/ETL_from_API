import pandas as pd
import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from prefect import flow
from prefect.states import Completed

from src.helpers.gold.extract import get_last_processed_timestamp
from src.helpers.gold.load import update_last_processed_timestamp
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.tasks.gold.extract_from_gold import get_daily_gold_azure


@flow(name="Aggregate daily to weekly flow")
def daily_to_weekly_aggregation(week_start: pendulum.DateTime = None):
    logger = get_logger()
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
            current_week_start = last_processed.add(weeks=1)
    else:
        current_week_start = week_start

    target_week_start = now.start_of("week").subtract(weeks=1)

    # ── early exit ────────────────────────────────────────────────
    if current_week_start > target_week_start:
        logger.info(
            f"No unprocessed weeks. "
            f"Last processed: {last_processed.to_date_string() if last_processed else 'None'}. "
            f"Skipping."
        )
        return Completed(message="Skipped-AlreadyProcessed")

    # ── catchup loop ──────────────────────────────────────────────
    all_weeks: dict[pendulum.DateTime, list[tuple[pendulum.DateTime, pd.DataFrame]]] = {}
    problematic_weeks: list[str] = []

    while current_week_start <= target_week_start:
        week_end = current_week_start.end_of("week")
        logger.info(f"Processing week: {current_week_start.to_date_string()} → {week_end.to_date_string()}")

        all_days_dfs, missing_days = get_daily_gold_azure(current_week_start, week_end)

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

        update_last_processed_timestamp(pipeline_name, current_week_start)
        logger.info(f"Week {current_week_start.to_date_string()} marked as processed.")

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

    for week_start, days in all_weeks.items():
        logger.info(f"Aggregating week {week_start.to_date_string()} — {len(days)} days")
        pass
        # weekly_summary = get_weekly_summ_data(days)
        # load_gold_weekly_summ_data_to_azure(pipeline_name, weekly_summary)
        # load_gold_weekly_summ_data_to_postgres(weekly_summary)


        # daily_to_weekly_aggregation(week_start=pendulum.datetime(2026, 2, 24))  # manual reprocess

        # daily_summ_result = get_weekly_summ_data(gold_result)
        #
        # load_gold_weekly_summ_data_to_azure(pipeline_name, daily_summ_result)
        # load_gold_weekly_summ_data_to_postgres(daily_summ_result)
        #
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

