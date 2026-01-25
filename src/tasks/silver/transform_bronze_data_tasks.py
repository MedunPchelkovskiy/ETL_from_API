import pandas as pd
from prefect import task

from src.helpers.logging_helper.combine_loggers_helper import get_logger
from src.workers.silver.transform_bronze_data import parse_bronze_df, clean_silver_df


@task
def parse_api_group(api_name: str, api_df: pd.DataFrame, api_parsing: dict):
    logger = get_logger()
    logger.info(
        "Task parse api group started for api_name=%s",
        api_name,
    )

    parsed_frames = parse_bronze_df(api_name, api_df, api_parsing)

    logger.info(
        "Task parse api group completed for api_name=%s",
        api_name,
    )

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


# Task: validate and clean
@task
def validate_silver_data(df: pd.DataFrame) -> pd.DataFrame:  # TODO: add real validation with pandera
    if df is None or df.empty:
        return False

    required_cols = {"api_name", "place_name", "ingest_date", "ingest_hour"}
    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    return True
