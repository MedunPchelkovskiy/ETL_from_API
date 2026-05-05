import pandas as pd
import pendulum
from decouple import config
from prefect import runtime
from prefect import task
from sqlalchemy import create_engine

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.load_gold_data import load_gold_data_to_azure_worker, load_gold_daily_data_to_postgres_worker, \
    load_five_day_data_to_postgres_worker, load_gold_five_day_data_to_azure_worker, \
    load_daily_summ_data_to_azure_worker, load_gold_daily_summ_data_to_postgres_worker, \
    load_weekly_summ_data_to_azure_worker, load_gold_weekly_summ_data_to_postgres_worker, \
    load_monthly_summ_data_to_azure_worker


@task(name="Load gold daily data to Azure blob", retries=3, retry_delay_seconds=300)
def load_gold_daily_data_to_azure(pipeline_name, gold_result: list):
    logger = get_logger()
    logger.info("Start task loading gold data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

    load_gold_data_to_azure_worker(pipeline_name, gold_result)

    logger.info("Completed task loading gold daily data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )


@task(retries=3, retry_delay_seconds=300)
def load_gold_daily_data_to_postgres(gold_result: list):
    logger = get_logger()
    logger.info("Start task loading gold daily data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    engine = create_engine(config("DB_CONN_RAW"))
    load_gold_daily_data_to_postgres_worker(gold_result, engine)
    logger.info("Completed task loading gold daily data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )


@task(name="Load five day gold data to Azure blob", retries=3, retry_delay_seconds=300)
def load_gold_five_day_data_to_azure(pipeline_name, gold_result: list):
    logger = get_logger()
    logger.info("Start task loading gold five day data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

    load_gold_five_day_data_to_azure_worker(pipeline_name, gold_result)

    logger.info("Completed task loading gold five day data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )


@task(retries=3, retry_delay_seconds=300)
def load_gold_five_day_data_to_postgres(fd_gold_result: list):
    logger = get_logger()
    logger.info("Start task loading gold five day data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    engine = create_engine(config("DB_CONN_RAW"))
    load_five_day_data_to_postgres_worker(fd_gold_result, engine)
    logger.info("Completed task loading gold five day data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )



@task(name="Load gold daily summarized data to Azure blob", retries=3, retry_delay_seconds=300)
def load_gold_daily_summ_data_to_azure(pipeline_name, gold_result: list):
    logger = get_logger()
    logger.info("Start task loading gold data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

    load_daily_summ_data_to_azure_worker(pipeline_name, gold_result)

    logger.info("Completed task loading gold daily data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

@task(retries=3, retry_delay_seconds=300)
def load_gold_daily_summ_data_to_postgres(gold_result: list[tuple[pendulum.DateTime, pd.DataFrame]]):
    logger = get_logger()
    logger.info("Start task loading gold daily summarized data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    engine = create_engine(config("DB_CONN_RAW"))
    load_gold_daily_summ_data_to_postgres_worker(gold_result, engine)
    logger.info("Completed task loading gold daily summarized data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )


@task(name="Load gold weekly summarized data to Azure blob", retries=3, retry_delay_seconds=30)
def load_gold_weekly_summ_data_to_azure(pipeline_name, week: pd.DataFrame):
    logger = get_logger()
    logger.info("Start task loading gold weekly data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    load_weekly_summ_data_to_azure_worker(pipeline_name, week)

    logger.info("Completed task loading gold weekly data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )

@task(name="Load gold weekly summarized data to postgres", retries=3, retry_delay_seconds=60)
def load_gold_weekly_summ_data_to_postgres(pipeline_name, all_weeks_summ: list[pd.DataFrame]):
    logger = get_logger()
    logger.info("Start task loading gold weekly data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    engine = create_engine(config("DB_CONN_RAW"))
    load_gold_weekly_summ_data_to_postgres_worker(engine, all_weeks_summ)

    logger.info("Completed task loading gold weekly data to Postgres local",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )




@task(name="Load gold weekly summarized data to Azure blob", retries=3, retry_delay_seconds=30)
def load_gold_monthly_summ_data_to_azure(pipeline_name, month: pd.DataFrame):
    logger = get_logger()
    logger.info("Start task loading gold weekly data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )
    load_monthly_summ_data_to_azure_worker(pipeline_name, month)

    logger.info("Completed task loading gold weekly data to Azure",
                extra={"flow_run_id": runtime.flow_run.id,
                       "task_run_id": runtime.task_run.id,
                       }
                )





