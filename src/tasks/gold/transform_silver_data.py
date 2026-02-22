import pendulum
from prefect import task

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.transform_silver_data import get_df_data, get_fdf_data


@task(name="Transform silver data to daily")
def get_daily_forecast_data(silver_results: list, generated_at: pendulum.DateTime):
    logger = get_logger()
    logger.info(f"Start task get daily forecast data", extra={})
    df_data = get_df_data(silver_results, generated_at)
    rows = len(df_data)
    logger.info(f"End task get daily forecast data", extra={"rows_count": rows})
    return df_data


@task(name="Transform silver data to five days")
def get_five_day_forecast_data(silver_df):
    logger = get_logger()
    logger.info(f"Start task five day forecast data", extra={})
    fdf_data = get_fdf_data(silver_df)
    rows = len(fdf_data)
    logger.info(f"End task five day forecast data", extra={"rows_count": rows})
    return fdf_data
