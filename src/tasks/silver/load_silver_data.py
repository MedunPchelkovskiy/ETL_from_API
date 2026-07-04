from decouple import config
from prefect import runtime
from prefect import task
from sqlalchemy import create_engine

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.decorators import measure_task_duration
from src.helpers.observability_helpers.pushgateway_utils import push_task_metrics
from src.workers.silver.load_transformed_data_to_azure import load_silver_data_to_azure_worker
# from load.raw_data.workers.load_raw_data_from_weather_APIs_to_Azure import upload_json
from src.workers.silver.load_transformed_data_to_local_postgres import load_silver_data_to_postgres_worker


@task(retries=3, retry_delay_seconds=5)
@measure_task_duration(flow_name="silver_flow", task_name="load_silver_to_azure", on_complete=push_task_metrics)
def load_silver_data_to_azure(df):
    logger = get_logger()
    logger.info("Start task loading transformed data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

    load_silver_data_to_azure_worker(df)

    logger.info("Completed task loading transformed data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )


@task(retries=3, retry_delay_seconds=5)
@measure_task_duration(flow_name="silver_flow", task_name="load_silver_to_postgres", on_complete=push_task_metrics)
def load_silver_data_to_postgres(data):
    logger = get_logger()
    logger.info("Start task loading transformed data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    engine = create_engine(config("DB_CONN_RAW"))
    load_silver_data_to_postgres_worker(data, engine)
    logger.info("Completed task loading transformed data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
