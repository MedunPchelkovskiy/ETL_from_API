from decouple import config
from prefect import task, runtime
from sqlalchemy import create_engine

from src.helpers.logging_helper.combine_loggers_helper import get_logger
from src.workers.silver.extract_bronze_data_for_transformation import extract_bronze_data_from_postgres
# from load.raw_data.workers.load_raw_data_from_weather_APIs_to_Azure import upload_json

@task(retries=3, retry_delay_seconds=7)
def extract_bronze_data(date, hour):
    """
    Prefect task to extract raw JSON rows from Postgres.
    """
    logger = get_logger()
    logger.info("Start task extract bronze data from Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

    # Create engine inside task to avoid Prefect caching issues
    engine = create_engine(config("DB_CONN"))

    data = extract_bronze_data_from_postgres(engine, date, hour)

    logger.info("Completed extract bronze data from Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    return data






# @task(retries=3, retry_delay_seconds=7)
# def extract_bronze_data(date, hour):
#     engine = create_engine(config("DB_CONN"))
#     logger = get_logger()
#     logger.info("Start task extract bronze data from Postgres local",
#                 extra={"flow_run_id": runtime.flow_run.id,
#                        "task_run_id": runtime.task_run.id,
#                        }
#                 )
#     data = extract_bronze_data_from_postgres(engine, date, hour)
#     logger.info("Completed extract bronze data from Postgres local",
#                 extra={"flow_run_id": runtime.flow_run.id,
#                        "task_run_id": runtime.task_run.id,
#                        }
#                 )
#
#     return data
