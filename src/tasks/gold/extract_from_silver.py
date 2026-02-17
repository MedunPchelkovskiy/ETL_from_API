from datetime import datetime

import pandas as pd
import pendulum
import prefect
from decouple import config
from fabric.decorators import task
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, DBAPIError, SQLAlchemyError

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.extract_silver_data import fetch_silver_parquet_blob, fetch_silver_data_postgres


@task(name="Get silver data from azure", retries=3, retry_delay_seconds=60)
def get_silver_parquet_azure():
    logger = get_logger()
    start_time = pendulum.now("UTC")
    now = start_time
    year, month, day, hour = now.format("YYYY"), now.format("MM"), now.format("DD"), now.format("HH")
    logger.info(f"Start task get silver parquet azure for {year}-{month}-{day} {hour}:00",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                }
                )
    try:
        silver_parquet = fetch_silver_parquet_blob(year, month, day, hour)
        silver_df = pd.read_parquet(silver_parquet)
    except FileNotFoundError:
        logger.error(f"Silver parquet not found for {year}-{month}-{day}-{hour}")
        raise
    except Exception as e:
        logger.error(f"Failed to get silver parquet: {e}")
        raise

    duration = (pendulum.now("UTC") - start_time).total_seconds()
    logger.info(
        "Finished task get silver parquet azure",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "task_run_id": prefect.runtime.task_run.id,
            "rows_loaded": len(silver_df),
            "duration_seconds": duration
        }
    )

    return silver_df


@task(name="daily forecast")
def get_silver_data_postgres():
    logger = get_logger()
    now = datetime.now()
    hour_str = now.strftime("%H")
    date = now.strftime("%Y-%m-%d")  # canonical
    hour_int = int(hour_str)

    logger.info("Start task get silver data postgres",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id
                })
    engine = create_engine(config("DB_CONN_RAW"))
    silver_df = None
    try:
        silver_df = fetch_silver_data_postgres(date, hour_int, engine)


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
