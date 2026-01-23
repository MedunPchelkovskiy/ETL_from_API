from prefect import runtime
from prefect import task

from src.helpers.logging_helper.combine_loggers_helper import get_logger
from src.workers.silver.extract_bronze_data_for_transformation import extract_bronze_data_from_postgres
# from load.raw_data.workers.load_raw_data_from_weather_APIs_to_Azure import upload_json
from src.workers.silver.load_transformed_data_from_weather_api_to_local_postgres import \
    load_transformed_api_data_to_postgres


@task
def extract_bronze_data_for_transformation_from_postgres():
    logger = get_logger()
    logger.info("Start task extract bronze data from Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    data = extract_bronze_data_from_postgres()
    logger.info("Completed extract bronze data from Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

    return data
