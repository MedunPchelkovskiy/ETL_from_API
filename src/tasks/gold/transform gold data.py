import pendulum
from prefect import task

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.transform_gold_data import get_daily_summ_data_worker


@task(name="Transform silver data to daily")
def get_dail_data(gold_results: list, generated_at: pendulum.DateTime
                            ):
    logger = get_logger()
    logger.info(f"Start task get daily forecast data", extra={})

    df_data = get_daily_summ_data_worker(gold_results)
    rows = len(df_data)

    logger.info(f"End task get daily forecast data", extra={"rows_count": rows})
    return df_data