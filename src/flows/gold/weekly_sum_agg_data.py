import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from prefect import flow
from prefect.states import Completed

from src.helpers.logging_helpers.combine_loggers_helper import get_logger


@flow
def daily_to_weekly_aggregation(forecast_day=None):
    logger = get_logger()
    now = pendulum.now("UTC")
    logger.info(f"Starting aggregate hourly to daily flow",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "utc_time": now.to_iso8601_string(),
                })

    pipeline_name = "Aggregate hourly to daily flow"

    if forecast_day is None:
        forecast_day = now.subtract(days=1).start_of("day")



    try:
        # task return list of tuples (day, df) for missing days
        gold_result = get_daily_gold_azure(pipeline_name, forecast_day)
        logger.info(f"Downloaded gold blob data, must be list of df's: {gold_result}")
    except ResourceNotFoundError as e:
        logger.info(
            f"No parquet files found for day {forecast_day.format('DD')}, fall back to postgres | error={e}",
            extra={
                "flow_run_id": prefect.runtime.flow_run.id,
                "task_run_id": prefect.runtime.task_run.id
            })
        gold_result = get_daily_gold_postgres(pipeline_name, forecast_day)
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

