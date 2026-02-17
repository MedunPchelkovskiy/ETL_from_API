from decouple import config
from prefect import runtime
from prefect import task
from sqlalchemy import create_engine

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.load_gold_data import load_gold_data_to_azure_worker, load_gold_daily_data_to_postgres_worker, \
    load_five_day_data_to_postgres_worker


@task(name="Load gold data to Azure blob", retries=3, retry_delay_seconds=300)
def load_gold_data_to_azure(df):
    logger = get_logger()
    logger.info("Start task loading gold data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

    load_gold_data_to_azure_worker(df)

    logger.info("Completed task loading gold data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )


@task(retries=3, retry_delay_seconds=300)
def load_gold_daily_data_to_postgres(data):
    logger = get_logger()
    logger.info("Start task loading gold data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    engine = create_engine(config("DB_CONN_RAW"))
    load_gold_daily_data_to_postgres_worker(data, engine)
    logger.info("Completed task loading gold data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )


@task(retries=3, retry_delay_seconds=300)
def load_gold_five_day_data_to_postgres(data):
    logger = get_logger()
    logger.info("Start task loading gold data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    engine = create_engine(config("DB_CONN_RAW"))
    load_five_day_data_to_postgres_worker(data, engine)
    logger.info("Completed task loading gold data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
