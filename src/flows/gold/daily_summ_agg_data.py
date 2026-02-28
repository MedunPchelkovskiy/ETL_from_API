import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from prefect import flow

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.tasks.gold.extract_from_gold import get_hourly_gold_azure
from src.tasks.gold.load_gold_data import load_gold_daily_summ_data_to_azure
from src.tasks.gold.transform_gold_data import get_daily_summarized_data


@flow(name="Aggregate hourly to daily flow")
def hourly_to_daily_aggregation(forecast_day=None):
    logger = get_logger()
    now = pendulum.now("UTC")
    pipeline_name = "Aggregate hourly to daily flow"

    if forecast_day is None:
        forecast_day = now.subtract(days=1)

    logger.info(f"Starting aggregate hourly to daily flow",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "utc_time": now.to_iso8601_string(),
                })

    gold_result = []
    try:
        # task return list of tuples (day, df) for missing days
        gold_result = get_hourly_gold_azure(pipeline_name, forecast_day)
    except ResourceNotFoundError as e:
        logger.info(
            f"No parquet files found for day {forecast_day.format('DD')}, fall back to postgres | error={e}",
            extra={
                "flow_run_id": prefect.runtime.flow_run.id,
                "task_run_id": prefect.runtime.task_run.id
            })
        # gold_result = get_silver_data_postgres(forecast_day)
    logger.info(f"Downloaded gold result data, must be list of df's: {gold_result}")

    if not gold_result:
        logger.info("No new Gold data to process.")

    daily_summ_result = get_daily_summarized_data(gold_result)

    load_gold_daily_summ_data_to_azure(pipeline_name, daily_summ_result)
    load_gold_daily_data_to_postgres(daily_result)

    # print first 5 rows vertically for dev logs
    for i, (ts, df) in enumerate(daily_result[:5]):
        logger.info("\nItem %s timestamp: %s", i, ts)
        logger.info(
            "\nItem %s dataframe (first row vertical):\n%s",
            i,
            df.iloc[0].to_frame().to_string() if not df.empty else "Empty DataFrame"
        )

    logger.info(f"End flow daily dataset forecast",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "daily_rows_loaded": len(daily_result),
                    # "five_day_rows_loaded": len(five_day_result),
                    "utc_time": pendulum.now("UTC").to_iso8601_string()
                })


if __name__ == "__main__":
    hourly_to_daily_aggregation()
