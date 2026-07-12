from venv import logger

import pandas as pd
import pendulum
from prefect import task
from sqlalchemy_utils.types import password

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.decorators import measure_task_duration
from src.helpers.observability_helpers.pushgateway_utils import push_task_metrics
from src.workers.gold.transform_gold_data import get_daily_summ_data_worker, get_weekly_summ_data_worker, \
    get_monthly_summ_data_worker, aggregate_months_to_year, aggregate_months_to_season


@task(name="Transform gold data to daily")
@measure_task_duration(flow_name="gold_daily_summ_flow", task_name="get_daily_summ_data", on_complete=push_task_metrics)
def get_daily_summ_data(gold_results: list[tuple[pendulum.DateTime, pd.DataFrame]]) -> list[
    tuple[pendulum.DateTime, pd.DataFrame]]:
    logger = get_logger()
    logger.info(f"Start task get daily forecast data", extra={})

    daily_summ_data = get_daily_summ_data_worker(gold_results)
    rows = len(daily_summ_data)

    logger.info(f"End task get daily forecast data", extra={"rows_count": rows})
    return daily_summ_data


@task(name="Transform gold daily data to weekly")
@measure_task_duration(flow_name="gold_weekly_summ_flow", task_name="get_weekly_summ_data", on_complete=push_task_metrics)
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
@measure_task_duration(flow_name="gold_monthly_summ_flow", task_name="get_monthly_summ_data", on_complete=push_task_metrics)
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


@task(name="Transform gold monthly data to yearly",
      task_run_name="Transform monthly | {year}")
@measure_task_duration(flow_name="gold_monthly_to_yearly_flow", task_name="aggregate_gold_months", on_complete=push_task_metrics)
def aggregate_gold_months(all_months_dfs, expected_months, max_missing_ratio, year) -> pd.DataFrame:
    logger = get_logger()
    logger.info(f"Start task aggregate monthly gold data")
    aggregated_months_dfs = aggregate_months_to_year(all_months_dfs, expected_months, max_missing_ratio, year)
    logger.info(f"End task aggregate monthly gold data")

    return aggregated_months_dfs



@task(name="Transform gold monthly data to seasonally",
      task_run_name="Transform monthly to seasonally | {year}")
@measure_task_duration(flow_name="gold_seasonal_flow", task_name="get_seasonally_summ_data", on_complete=push_task_metrics)
def get_seasonally_summ_data(season, year, data) -> pd.DataFrame:
    logger = get_logger()
    logger.info(f"Start task aggregate monthly to seasonally data for {season}_{year}")
    aggregated_season = aggregate_months_to_season(season, year, data)
    logger.info(f"End task aggregate monthly to seasonally data for {season}_{year}")

    return aggregated_season
