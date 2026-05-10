import pendulum
import prefect
from decouple import config
from prefect import flow
from sqlalchemy import create_engine

from src.clients.datalake_client import fs_client
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.pipeline_config import PIPELINE_CONFIG
from src.helpers.observability_helpers.state_helpers import get_last_reconciled_date, reconcile_processing_state

PIPELINE_NAME = "gold_yearly"


@flow(
    name="Aggregate monthly to yearly flow",
)
def monthly_to_yearly_aggregation():
    logger = get_logger()
    now = pendulum.now("UTC")
    engine = create_engine(config("DB_CONN_RAW"))
    cfg = PIPELINE_CONFIG[PIPELINE_NAME]
    max_missing_ratio = cfg["max_missing_ratio"]

    logger.info(
        f"Starting {PIPELINE_NAME}",
        extra={
            "flow_run_id": prefect.runtime.flow_run.id,
            "utc_time": now.to_iso8601_string(),
        },
    )

    now = pendulum.now("UTC")
    end_date = now.start_of("month")  # текущият месец excluded

    last_reconciled = get_last_reconciled_date(PIPELINE_NAME)

    if last_reconciled is None:
        reconcile_start = pendulum.datetime(now.year, 1, 1)
        logger.info(f"[{PIPELINE_NAME}] First run — reconciling from {reconcile_start.to_date_string()}")
    else:
        reconcile_start = last_reconciled
        logger.info(
            f"[{PIPELINE_NAME}] Reconciling from {reconcile_start.to_date_string()} "
            f"→ {end_date.to_date_string()}"
        )

    reconcile_processing_state(
        pipeline_name=PIPELINE_NAME,
        start_date=reconcile_start,
        end_date=end_date,
        fs_client=fs_client,
        engine=engine,
    )
