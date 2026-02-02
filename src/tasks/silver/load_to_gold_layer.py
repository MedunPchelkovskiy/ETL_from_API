from decouple import config
from prefect import runtime
from prefect import task
from sqlalchemy import create_engine

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
# from load.raw_data.workers.load_raw_data_from_weather_APIs_to_Azure import upload_json
from src.workers.silver.load_transformed_data_to_local_postgres import load_silver_data_to_postgres_worker


@task(retries=3, retry_delay_seconds=5)
def load_silver_data_to_postgres(data):
    logger = get_logger()
    logger.info("Start task loading transformed data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    engine = create_engine(config("DB_CONN"))
    load_silver_data_to_postgres_worker(data, engine)
    logger.info("Completed task loading transformed data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )


@task(retries=3, retry_delay_seconds=5)
def load_silver_data_to_azure(data, engine):
    logger = get_logger()
    logger.info("Start task loading transformed data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

    # load_silver_data_to_azure_worker(data, engine)
    # logger.info("Completed task loading transformed data to Postgres local",
    #             extra={"flow_run_id": runtime.flow_run.id,
    #                    "task_run_id": runtime.task_run.id,
    #                    }
    #             )
