import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from prefect import flow

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.tasks.gold.extract_from_silver import get_silver_parquet_azure, get_silver_data_postgres
from src.tasks.gold.load_gold_data import load_gold_five_day_data_to_postgres
from src.tasks.gold.transform_silver_data import get_five_day_forecast_data


@flow(name="five day forecast")
def five_day_forecast(debug: bool = False):
    logger = get_logger()

    if debug:
        logger.info("Running in Debug mode")

    now = pendulum.now("UTC")
    date = now.format("YYYY-MM-DD")
    hour_str = now.format("HH")

    logger.info(f"Starting flow five day dataset forecast.",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "utc_time": now.to_iso8601_string(),
                })
    silver_records = None
    try:
        silver_records = get_silver_parquet_azure()
    except ResourceNotFoundError as e:
        logger.info(f"No parquet file found for {date}-{hour_str}, fall back to postgres | error={e}",
                    extra={
                        "flow_run_id": prefect.runtime.flow_run.id,
                        "task_run_id": prefect.runtime.task_run.id
                    })
        silver_records = get_silver_data_postgres()

    df_data = get_five_day_forecast_data(silver_records)
    # load_gold_data_to_azure(df_data)
    load_gold_five_day_data_to_postgres(df_data)
