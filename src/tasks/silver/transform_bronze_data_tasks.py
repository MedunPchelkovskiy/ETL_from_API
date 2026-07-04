import pandas as pd
from prefect import task

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.decorators import measure_task_duration
from src.helpers.observability_helpers.pushgateway_utils import push_task_metrics
from src.workers.silver.transform_bronze_data import clean_silver_df, normalize_and_combine, parse_records_from_api


@task
def normalize_combine_task(download_results):
    combined_df = normalize_and_combine(download_results)

    return combined_df


@task
@measure_task_duration(flow_name="silver_flow", task_name="parse_api_records", on_complete=push_task_metrics)
def parse_api_records(bronze_records: dict):
    """
    Prefect task that parses one API's group of bronze rows.
    Uses the professional API-specific parser from `api_parsing`.
    """
    logger = get_logger()
    logger.info("Task parse api group started | Rows=%s", len(bronze_records))

    silver_df = parse_records_from_api(bronze_records)

    logger.info("Task parse api group completed | Parsed rows=%s", len(silver_df))
    return silver_df


@task
@measure_task_duration(flow_name="silver_flow", task_name="clean_silver", on_complete=push_task_metrics)
def clean_silver(df: pd.DataFrame):
    logger = get_logger()
    logger.info(
        "Task clean silver df started ...",
    )
    cleaned_df = clean_silver_df(df)
    logger.info(
        "Task clean silver df completed",
    )

    return cleaned_df

# # Task: validate and clean
# @task
# def validate_silver_data(df: pd.DataFrame):
#     logger = get_logger()
#     logger.info("Task validation of silver df started ...")
#
#     try:
#         validate_silver_df(df)  # raises if invalid
#     except Exception as e:
#         logger.error("Silver DataFrame validation failed: %s", e, exc_info=True)
#         raise  # Let Prefect handle retries/failure
#
#     logger.info("Task validation of silver df completed")
#     return df
