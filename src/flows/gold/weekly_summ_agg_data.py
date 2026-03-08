import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from prefect import flow
from prefect.states import Completed

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.tasks.gold.extract_from_gold import get_daily_gold_azure


@flow(name="Aggregate daily to weekly flow")
def daily_to_weekly_aggregation(week_start=None):
    logger = get_logger()
    now = pendulum.now("UTC")

    if week_start is None:
        # Always deterministic: run Monday 00:05 → grab previous Mon–Sun
        week_start = now.start_of("week").subtract(weeks=1)  # last Monday 00:00
    week_end = week_start.end_of("week")  # last Sunday 23:59:59
    logger.info(f"Starting aggregate hourly to daily flow",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "utc_time": now.to_iso8601_string(),
                })

    pipeline_name = "Aggregate hourly to daily flow"

    try:
        # task return list of tuples (day, df) for missing days
        gold_result = get_daily_gold_azure(pipeline_name, week_start, week_end)
        logger.info(f"Downloaded gold blob data, must be list of df's: {gold_result}")
    except ResourceNotFoundError as e:
        logger.info(
            f"No parquet files found for day {week_start.format('DD')}, fall back to postgres | error={e}",
            extra={
                "flow_run_id": prefect.runtime.flow_run.id,
                "task_run_id": prefect.runtime.task_run.id
            })
        gold_result = get_daily_gold_postgres(pipeline_name, week_start)
        logger.info(f"Downloaded gold postgres data, must be list of df's: {gold_result}")

    if not gold_result:
        logger.info("No new Gold daily data to process. Skipping downstream tasks.")
        return Completed(name="Skipped-NoNewData")

    daily_summ_result = get_weekly_summ_data(gold_result)

    load_gold_weekly_summ_data_to_azure(pipeline_name, daily_summ_result)
    load_gold_weekly_summ_data_to_postgres(daily_summ_result)

    # print first 5 rows vertically for dev logs
    for i, (ts, df) in enumerate(daily_summ_result[:5]):
        logger.info("\nItem %s timestamp: %s", i, ts)
        logger.info(
            "\nItem %s dataframe (first row vertical):\n%s",
            i,
            df.iloc[0].to_frame().to_string() if not df.empty else "Empty DataFrame"
        )

    logger.info(f"End weekly aggregation flow",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "daily_rows_loaded": len(daily_summ_result),
                    # "five_day_rows_loaded": len(five_day_result),
                    "utc_time": pendulum.now("UTC").to_iso8601_string()
                })


if __name__ == "__main__":
    daily_to_weekly_aggregation()

