from prefect import flow, runtime
from prefect.deployments import run_deployment

from logging_config import setup_logging
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helpers.decorators import measure_flow_duration
from src.helpers.observability_helpers.processing_state_reader import get_processing_state_metrics
from src.helpers.observability_helpers.pushgateway_utils import push_processing_state_metrics


@flow(name="OrchestratorFlow")
@measure_flow_duration(flow_name="orchestrator_flow")
def orchestrator_flow():
    """
    Orchestrates two Prefect 3 deployments sequentially with structured logging.
    Logs include flow_run_id and task_run_id like your example.
    """
    # Get logger and run context
    setup_logging()
    logger = get_logger()
    try:
        # --- Run bronze deployment ---
        logger.info(
            "Starting First Flow Deployment...",
            extra={
                "flow_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "weather-flow-run/local-dev"
            }
        )
        first_result = run_deployment("weather-flow-run/bronze-flow")

        logger.info(
            "Completed First Flow Deployment",
            extra={
                "flow_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "weather-flow-run/local-dev",
                "state": first_result.state.type.value
            }
        )

        # --- Run silver deployment ---
        logger.info(
            "Starting Silver Flow Deployment...",
            extra={
                "flow_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "transform-bronze-data/SecondFlowDeployment"
            }
        )
        second_result = run_deployment("transform-bronze-data/silver-flow")
        logger.info(
            "Completed Silver Flow Deployment",
            extra={
                "flow_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "transform-bronze-data/silver-flow",
                "state": second_result.state.type.value
            }
        )

        # --- Run gold forecast deployment ---
        logger.info(
            "Starting Gold Flow Deployment...",
            extra={
                "flow_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "daily_dataset_forecast/gold-flow"
            }
        )
        gold_result = run_deployment("daily_dataset_forecast/gold-flow")
        logger.info(
            "Completed Gold Flow Deployment",
            extra={
                "flow_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "daily_dataset_forecast/gold-flow",
                "state": gold_result.state.type.value
            }
        )


    finally:
        logger.info("Entering processing_state metrics push")
        try:
            rows = get_processing_state_metrics()
            logger.info(f"Got {len(rows)} rows from processing_state", extra={"row_count": len(rows)})
            push_processing_state_metrics(flow_name="orchestrator_flow", rows=rows)
            logger.info("Processing_state metrics pushed successfully")
        except Exception as e:
            logger.error(f"Failed to push processing_state metrics: {e}")


if __name__ == "__main__":
    orchestrator_flow()
