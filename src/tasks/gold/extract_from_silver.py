from sqlite3 import OperationalError

import pendulum
import prefect
from decouple import config
from fabric.decorators import task
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from src.clients.datalake_client import fs_client
from src.helpers.gold.extract import get_last_processed_timestamp
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.extract_silver_data import fetch_silver_parquet_blob, fetch_silver_data_postgres


@task(name="Get silver data from Azure Parquet Incremental", retries=3, retry_delay_seconds=60)
def get_silver_parquet_azure(pipeline_name, forecast_day, max_hour):
    logger = get_logger()
    start_time = pendulum.now("UTC")

    last_processed_ts = get_last_processed_timestamp(pipeline_name)
    logger.info("Last processed TS: {}".format(last_processed_ts))

    if last_processed_ts is None:
        last_processed_ts = (pendulum.now("UTC").start_of("hour").subtract(days=1))
        logger.info(f"If last processed is None.")

    target_ts = forecast_day.replace(
        hour=int(max_hour.format("HH")),
        minute=0,
        second=0,
        microsecond=0
    )

    if last_processed_ts >= target_ts:
        logger.info("No new Silver files to process.")
        return []

    result = []
    current_ts = last_processed_ts.start_of("hour").add(hours=1)

    while current_ts <= target_ts:

        year = current_ts.format("YYYY")
        month = current_ts.format("MM")
        day = current_ts.format("DD")
        hour = current_ts.format("HH")

        logger.info(f"Processing Silver {year}-{month}-{day} {hour}:00")

        try:
            df = fetch_silver_parquet_blob(
                year=year,
                month=month,
                day=day,
                hour=hour,
                fs_client=fs_client
            )

            if not df.empty:
                result.append((current_ts, df))
            else:
                logger.warning(f"Empty Silver file for {year}-{month}-{day} {hour}")

        except Exception as e:
            logger.warning(
                f"Missing Silver file for {year}-{month}-{day} {hour} | error={e}"
            )

        current_ts = current_ts.add(hours=1)

    duration = (pendulum.now("UTC") - start_time).total_seconds()

    logger.info(
        f"Finished incremental Silver download. "
        f"Windows processed: {len(result)}. "
        f"Duration: {duration:.2f}s"
    )

    return result


@task(name="daily forecast")
def get_silver_data_postgres(forecast_day, max_hour):
    logger = get_logger()

    logger.info("Start task get silver data postgres",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id
                })
    engine = create_engine(config("DB_CONN_RAW"))
    silver_df = None
    try:
        silver_df = fetch_silver_data_postgres(forecast_day.format('YYYY-MM-DD'), max_hour.hour, engine)


    except OperationalError as e:
        logger.error(f"DB connection failed: {e}")

    except DBAPIError as e:
        logger.error(f"DB query error: {e}")

    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error: {e}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    end = pendulum.now("UTC")

    logger.info(
        "Finished task get silver data postgres",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "task_run_id": prefect.runtime.task_run.id,
        }
    )
    return silver_df
