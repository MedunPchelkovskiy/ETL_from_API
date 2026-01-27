import pandas as pd
from prefect import task

from src.helpers.logging_helper.combine_loggers_helper import get_logger
from src.workers.silver.transform_bronze_data import clean_silver_df


@task
def parse_api_group(api_name: str, api_df: pd.DataFrame, api_parsing: dict):
    """
    Prefect task that parses one API's group of bronze rows.
    Uses the professional API-specific parser from `api_parsing`.
    """
    logger = get_logger()
    logger.info("Task parse api group started for api_name=%s | Rows=%s", api_name, len(api_df))

    if api_df.empty:
        logger.warning("Empty dataframe for API: %s", api_name)
        return None

    parser = api_parsing.get(api_name)
    if not parser:
        logger.warning("No parser found for API: %s", api_name)
        return None

    parsed_frames = parser(api_df)

    if parsed_frames is None or parsed_frames.empty:
        logger.warning("Parser returned empty for API: %s", api_name)
        return None

    logger.info("Task parse api group completed for api_name=%s | Parsed rows=%s", api_name, len(parsed_frames))
    return parsed_frames


@task
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
