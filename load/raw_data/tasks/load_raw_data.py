from prefect import runtime
from prefect import task

from load.raw_data.workers.load_raw_data_from_weather_APIs_to_Azure import upload_json
from load.raw_data.workers.load_raw_data_from_weather_APIs_to_local_postgres import load_raw_api_data_to_postgres
from helpers.logging_helper.combine_loggers_helper import get_logger


@task
def load_raw_api_data_to_azure_blob(fs_client, base_dir, folder_name, file_name, data):
    logger = get_logger()
    logger.info("Start task loading raw data to Azure blob",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       "base_dir": base_dir,
                       "folder_name": folder_name,
                       "file_name": file_name
                       }
                )

    upload_json(fs_client, base_dir, folder_name, file_name, data)

    logger.info("Completed task loading raw data to Azure blob",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       "base_dir": base_dir,
                       "folder_name": folder_name
                       }
                )


@task
def load_raw_api_data_to_postgres_local(data, label):
    logger = get_logger()
    logger.info("Start task loading raw data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       "label": label
                       }
                )
    load_raw_api_data_to_postgres(data, label)
    logger.info("Completed task loading raw data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       "label": label
                       }
                )
