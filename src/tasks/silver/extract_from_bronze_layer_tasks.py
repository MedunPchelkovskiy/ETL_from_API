from decouple import config
from prefect import task, runtime
from sqlalchemy import create_engine

from src.helpers.logging_helper.combine_loggers_helper import get_logger
from src.workers.silver.extract_bronze_data_for_transformation import extract_bronze_data_from_postgres_worker, \
    download_json_from_adls_worker


@task(retries=3, retry_delay_seconds=7)
def extract_bronze_data_from_postgres(date, hour):
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

    raw_pg_df = extract_bronze_data_from_postgres_worker(engine, date, hour)

    logger.info("Completed extract bronze data from Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    return raw_pg_df


@task
def extract_bronze_data_from_azure_blob_task(azure_fs_client, base_dir, date, hour):
    logger = get_logger()
    logger.info("Start task extract bronze data from Azure blob",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

    raw_jsons = download_json_from_adls_worker(azure_fs_client, base_dir, date, hour)

    logger.info("Completed extract bronze data from Azure blob",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    return raw_jsons
