import pandas as pd
import pendulum
from prefect import task

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.transform_gold_data import get_daily_summ_data_worker, get_weekly_summ_data_worker, \
    get_monthly_summ_data_worker


@task(name="Transform gold data to daily")
def get_daily_summ_data(gold_results: list[tuple[pendulum.DateTime, pd.DataFrame]]) -> list[
    tuple[pendulum.DateTime, pd.DataFrame]]:
    logger = get_logger()
    logger.info(f"Start task get daily forecast data", extra={})

    daily_summ_data = get_daily_summ_data_worker(gold_results)
    rows = len(daily_summ_data)

    logger.info(f"End task get daily forecast data", extra={"rows_count": rows})
    return daily_summ_data


@task(name="Transform gold daily data to weekly")
def get_weekly_summ_data(week_start: pendulum.DateTime,
                         days: list[tuple[pendulum.DateTime, pd.DataFrame]]) -> pd.DataFrame:
    logger = get_logger()
    logger.info(f"Start task get weekly summ data")
    logger.info(f"Transforming week {week_start.to_date_string()}")
    try:
        curr_week_df = get_weekly_summ_data_worker(week_start, days)
        logger.info(f"End task get weekly summ data")
        return curr_week_df
    except Exception:
        logger.exception(f"Failed processing week {week_start}")
        raise


@task(name="Transform gold daily data to monthly",
      task_run_name="Transform monthly | {month_start}")
def get_monthly_summ_data(month_start: pendulum.DateTime,
                         days: list[tuple[pendulum.DateTime, pd.DataFrame]],
                          max_missing_ratio: float,) -> pd.DataFrame:
    logger = get_logger()
    start = pendulum.now("UTC")
    logger.info(f"Start task get monthly summ data")
    logger.info(f"Transforming month {month_start.to_date_string()}")
    try:
        curr_month_df = get_monthly_summ_data_worker(month_start, days, max_missing_ratio,)
        logger.info(f"End task get monthly summ data")
        return curr_month_df
    except Exception:
        logger.exception(f"Failed processing month {month_start}")
        raise
