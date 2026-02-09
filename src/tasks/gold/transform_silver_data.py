from prefect import task

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.transform_silver_data import get_df_data, get_fdf_data


@task(name="Transform silver data")
def get_daily_forecast_data(df):
    logger = get_logger()
    logger.info(f"Start task get daily forecast data", extra={})
    df_data = get_df_data(df)
    rows = len(df_data)
    logger.info(f"End task get daily forecast data", extra={"rows_count": rows})
    return df_data


@task(name="Transform silver data")
def get_five_day_forecast_data(df):
    logger = get_logger()
    logger.info(f"Start task five day forecast data", extra={})
    fdf_data = get_fdf_data(df)
    rows = len(fdf_data)
    logger.info(f"End task five day forecast data", extra={"rows_count": rows})
    return fdf_data
