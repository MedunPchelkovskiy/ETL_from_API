import pandas as pd
import pendulum
from prefect import task

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.transform_gold_data import get_daily_summ_data_worker, get_weekly_summ_data_worker


@task(name="Transform gold data to daily")
def get_daily_summ_data(gold_results: list[tuple[pendulum.DateTime, pd.DataFrame]]) -> list[
    tuple[pendulum.DateTime, pd.DataFrame]]:
    logger = get_logger()
    logger.info(f"Start task get daily forecast data", extra={})

    daily_summ_data = get_daily_summ_data_worker(gold_results)
    rows = len(daily_summ_data)

    logger.info(f"End task get daily forecast data", extra={"rows_count": rows})
    return daily_summ_data


@task(name="Transform gold data to daily")
def get_weekly_summ_data(week_start: pendulum.DateTime,
                         days: list[tuple[pendulum.DateTime, pd.DataFrame]]) -> pd.DataFrame:
    logger = get_logger()
    logger.info(f"Start task get weekly summ data")
    try:
        curr_week_df = get_weekly_summ_data_worker(week_start, days)
        logger.info(f"End task get weekly summ data")
        return curr_week_df
    except Exception:
        logger.exception(f"Failed processing week {week_start}")
        raise
