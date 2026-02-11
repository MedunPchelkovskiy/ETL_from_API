import time

from decouple import config
from prefect import task, runtime
from sqlalchemy import create_engine

from metrics import TASK_DURATION, ROWS_PROCESSED
from pushgateway_utils import push_task_metrics
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.silver.extract_bronze_data_for_transformation import extract_bronze_data_from_postgres_worker, \
    download_json_from_adls_worker


@task
def extract_bronze_data_from_azure_blob_task(azure_fs_client, base_dir, date, hour):
    logger = get_logger()
    task_start = time.time()

    # Log start of task
    logger.info(
        "Start task extract bronze data from Azure blob",
        extra={
            "flow_run_id": runtime.flow_run.id,
            "task_run_id": runtime.task_run.id,
            "date": date,
            "hour": hour
        }
    )

    # Extract data
    raw_records = download_json_from_adls_worker(azure_fs_client, base_dir, date, hour)

    # Compute row count safely
    rows_count = len(raw_records) if raw_records else 0

    # Update Prometheus metric
    # bronze_rows_extracted.labels(
    #     source="azure_blob",
    #     date=date,
    #     hour=str(hour),
    #     flow_run_id=runtime.flow_run.id,
    #     task_run_id=runtime.task_run.id
    # ).set(rows_count)

    # Optional: log anomaly only if zero rows
    if rows_count == 0:
        logger.warning(
            "Zero rows extracted from Azure blob",
            extra={
                "flow_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id,
                "date": date,
                "hour": hour
            }
        )

    logger.info("Extracted rows count from Azure blob: %s",
                rows_count)  # TODO: delete when implement metrics with prometheus!!!

    # Log completion
    logger.info(
        "Completed extract bronze data from Azure blob",
        extra={
            "flow_run_id": runtime.flow_run.id,
            "task_run_id": runtime.task_run.id,
            "rows_count": rows_count,
            "date": date,
            "hour": hour
        }
    )

    task_duration = time.time() - task_start
    push_task_metrics(flow_name="Transform bronze data", task_name="Extract raw jsons from Azure", duration=task_duration, rows=rows_count)
    # TASK_DURATION.labels("Transform bronze data", "Extract raw jsons from Azure").observe(time.time() - start)
    # ROWS_PROCESSED.labels("Transform bronze data", "Extract raw jsons from Azure").inc(rows_count)

    return raw_records


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

    raw_records = extract_bronze_data_from_postgres_worker(engine, date, hour)

    logger.info("Completed extract bronze data from Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    return raw_records
