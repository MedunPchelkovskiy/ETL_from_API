import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from prefect import flow

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.tasks.gold.extract_from_silver import get_silver_parquet_azure
from src.tasks.gold.load_gold_data import load_gold_data_to_azure, \
    load_gold_daily_data_to_postgres, load_gold_five_day_data_to_postgres
from src.tasks.gold.transform_silver_data import get_daily_forecast_data, get_five_day_forecast_data


@flow(
    name="daily_dataset_forecast", )
def daily_forecast(forecast_day=None, max_hour=None):  # possibly can pass old date and hour to ingest data
    logger = get_logger()
    now = pendulum.now("UTC")
    generated_at = now
    pipeline_name = "daily_dataset_forecast"

    if forecast_day is None:
        forecast_day = now

    if max_hour is None:
        max_hour = now

    logger.info(f"Starting flow daily dataset forecast",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "utc_time": now.to_iso8601_string(),
                })

    silver_result = []
    try:
        # task вече връща list of tuples (hour, df) за пропуснатите часове
        silver_result = get_silver_parquet_azure(pipeline_name, forecast_day, max_hour)
    except ResourceNotFoundError as e:
        logger.info(
            f"No parquet file found for day  {forecast_day.format('DD')}/{max_hour.hour}.parquet, fall back to postgres | error={e}",
            extra={
                "flow_run_id": prefect.runtime.flow_run.id,
                "task_run_id": prefect.runtime.task_run.id
            })
        # silver_result = get_silver_data_postgres(forecast_day, max_hour)  # закоментирано за момента
    logger.info(f"Downloaded silver result data, must be list of df's: {silver_result}")
    # ✅ Flatten резултата в един DataFrame
    # silver_df = flatten_incremental_silver(silver_result)

    if not silver_result:
        logger.info("No new Silver data to process after flattening.")

    daily_result = get_daily_forecast_data(silver_result, generated_at)
    five_day_result = get_five_day_forecast_data(silver_result, generated_at)

    load_gold_data_to_azure(pipeline_name, daily_result)
    load_gold_daily_data_to_postgres(daily_result)

    # load_gold_data_to_azure(pipeline_name, five_day_result)
    load_gold_five_day_data_to_postgres(five_day_result)

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
    daily_forecast()
