import pandas as pd
import pendulum
import prefect
from decouple import config
from prefect import task
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, DBAPIError, OperationalError

from src.clients.datalake_client import fs_client
from src.helpers.gold.extract import get_last_processed_timestamp
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.extract_gold_data import get_hourly_blobs_for_day, get_hourly_data_postgres


@task(name="Get hourly day data from azure")
def get_hourly_gold_azure(pipeline_name, forecast_day) -> list[tuple[pendulum.DateTime, pd.DataFrame]]:
    logger = get_logger()
    logger.info("Start task get hourly day data from azure")

    now = pendulum.now("UTC")
    start_time = now

    last_processed_ts = get_last_processed_timestamp(pipeline_name)
    logger.info("Last processed TS: {}".format(last_processed_ts))

    if last_processed_ts is None:
        last_processed_ts = now.start_of("week")
        current_ts = last_processed_ts.start_of("day")
        logger.info("First run detected. Starting from beginning of week.")
    else:
        current_ts = last_processed_ts.start_of("day").add(days=1)

    target_ts = forecast_day

    if current_ts > target_ts:
        logger.info("No new Gold files to process.")
        return []

    result = []

    while current_ts <= target_ts:

        year = current_ts.format("YYYY")
        month = current_ts.format("MM")
        day = current_ts.format("DD")

        logger.info(f"Processing Gold {year}-{month}-{day}")

        try:
            all_hours_dfs = get_hourly_blobs_for_day(year, month, day, fs_client)

            if all_hours_dfs.empty:
                logger.warning(f"Empty Gold df for {year}-{month}-{day}")
            else:
                if len(all_hours_dfs) < 24:
                    logger.warning(f"Missed {24 - len(all_hours_dfs)} Gold blobs for {year}-{month}-{day}.")
                result.append((current_ts, all_hours_dfs))

        except Exception as e:
            logger.warning(f"Failed to fetch Gold df for {year}-{month}-{day} | error={e}")

        current_ts = current_ts.add(days=1)

    duration = (pendulum.now("UTC") - start_time).total_seconds()

    logger.info(
        f"Finished incremental Gold download. "
        f"Windows processed: {len(result)}. "
        f"Duration: {duration:.2f}s"
    )

    return result


@task(name="daily forecast")
def get_hourly_gold_postgres(pipeline_name, forecast_day):
    logger = get_logger()

    logger.info("Start task get silver data postgres",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id
                })
    now = pendulum.now("UTC")
    start_time = now

    last_processed_ts = get_last_processed_timestamp(pipeline_name)
    logger.info("Last processed TS: {}".format(last_processed_ts))

    if last_processed_ts is None:
        current_ts = now.start_of("week")
        logger.info("First run detected. Starting from beginning of week.")
    else:
        current_ts = last_processed_ts.start_of("day").add(days=1)

    target_ts = forecast_day

    if current_ts >= target_ts:
        logger.info("No new Gold files to process.")
        return []

    result = []
    engine = create_engine(config("DB_CONN_RAW"))

    while current_ts <= target_ts:

        try:
            silver_df = get_hourly_data_postgres(current_ts.format('YYYY-MM-DD'), engine)
            result.append((current_ts, silver_df))
            current_ts = current_ts.add(days=1)

        except OperationalError as e:
            logger.error(f"DB connection failed: {e}")

        except DBAPIError as e:
            logger.error(f"DB query error: {e}")

        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error: {e}")

        except Exception as e:
            logger.error(f"Unexpected error: {e}")

    end = pendulum.now("UTC")

    duration = (end - start_time).total_seconds()

    logger.info(
        "Finished task get silver data postgres",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "task_run_id": prefect.runtime.task_run.id,
            "Duration": f"{duration:.2f}s"
        }
    )
    return result


def get_daily_gold_azure(pipeline_name, forecast_day):
    logger = get_logger()
    logger.info("Start task get daily summ data from azure")

    now = pendulum.now("UTC")
    start_time = now

    last_processed_ts = get_last_processed_timestamp(pipeline_name)
    logger.info("Last processed TS: {}".format(last_processed_ts))

    if last_processed_ts is None:
        last_processed_ts = now.start_of("week")
        current_ts = last_processed_ts.start_of("day")
        logger.info("First run detected. Starting from beginning of week.")
    else:
        current_ts = last_processed_ts.start_of("day").add(days=1)

    target_ts = forecast_day

    if current_ts > target_ts:
        logger.info("No new daily gold files to process.")
        return []

    result = []

    while current_ts <= target_ts:

        year = current_ts.format("YYYY")
        month = current_ts.format("MM")
        day = current_ts.format("DD")

        logger.info(f"Processing daily gold blob {year}-{month}-{day}")

        try:
            all_hours_dfs = get_hourly_blobs_for_day(year, month, day, fs_client)

            if all_hours_dfs.empty:
                logger.warning(f"Empty Gold df for {year}-{month}-{day}")
            else:
                if len(all_hours_dfs) < 24:
                    logger.warning(f"Missed {24 - len(all_hours_dfs)} Gold blobs for {year}-{month}-{day}.")
                result.append((current_ts, all_hours_dfs))

        except Exception as e:
            logger.warning(f"Failed to fetch Gold df for {year}-{month}-{day} | error={e}")

        current_ts = current_ts.add(days=1)

    duration = (pendulum.now("UTC") - start_time).total_seconds()

    logger.info(
        f"Finished incremental Gold download. "
        f"Windows processed: {len(result)}. "
        f"Duration: {duration:.2f}s"
    )

    return result

