import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from prefect import flow

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.tasks.gold.extract_from_silver import get_silver_parquet_azure, get_silver_data_postgres


@flow(name="five day forecast")
def five_day_forecast():
    logger = get_logger()

    now = pendulum.now("UTC")
    date = now.format("YYYY-MM-DD")
    hour_str = now.format("HH")

    logger.info(f"Starting flow daily dataset forecast",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "utc_time": now.to_iso8601_string(),
                })
    silver_df = None
    try:
        silver_df = get_silver_parquet_azure()
    except ResourceNotFoundError as e:
        logger.info(f"No parquet file found for {date}-{hour_str}, fall back to postgres | error={e}",
                    extra={
                        "flow_run_id": prefect.runtime.flow_run.id,
                        "task_run_id": prefect.runtime.task_run.id
                    })
        silver_df = get_silver_data_postgres()

