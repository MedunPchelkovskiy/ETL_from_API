from prefect import flow, runtime
from prefect.deployments import run_deployment

from logging_config import setup_logging
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helper.check_pipeline_status import try_acquire_pipeline_lock
from src.helpers.observability_helper.flow_state_assertion import assert_deployment_ok
from src.helpers.observability_helper.pipeline_status_set import set_pipeline_status


@flow(name="OrchestratorFlow")
def orchestrator_flow():
    """
    Orchestrates two Prefect 3 deployments sequentially with structured logging.
    Logs include flow_run_id and task_run_id like your example.
    """

    # Get logger and run context
    setup_logging()
    logger = get_logger()

    pipeline_name = "orchestrator_pipeline"
    run_id = runtime.flow_run.id

    if not try_acquire_pipeline_lock(pipeline_name, run_id):
        logger.info("Already running → skip")
        return

    set_pipeline_status(
        pipeline_name=pipeline_name,
        status="processing",
        run_id=run_id
    )
    try:

        # --- Run bronze deployment ---
        logger.info(
            "Starting First Flow Deployment...",
            extra={
                "orchestrator_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "weather-flow-run/local-dev"
            }
        )
        first_result = run_deployment("weather-flow-run/bronze-flow", timeout=900)
        assert_deployment_ok(first_result.state, logger, "bronze", runtime)

        logger.info(
            "Completed First Flow Deployment",
            extra={
                "orchestrator_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "weather-flow-run/local-dev",
                "state": first_result.state.type.value
            }
        )

        # --- Run silver deployment ---
        logger.info(
            "Starting Silver Flow Deployment...",
            extra={
                "orchestrator_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "transform-bronze-data/SecondFlowDeployment"
            }
        )
        second_result = run_deployment("transform-bronze-data/silver-flow", timeout=900)
        assert_deployment_ok(second_result.state, logger, "silver", runtime)

        logger.info(
            "Completed Silver Flow Deployment",
            extra={
                "orchestrator_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "transform-bronze-data/silver-flow",
                "state": second_result.state.type.value
            }
        )

        # --- Run gold forecast deployment ---
        logger.info(
            "Starting Gold Flow Deployment...",
            extra={
                "orchestrator_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "daily_dataset_forecast/gold-flow"
            }
        )
        gold_result = run_deployment("daily_dataset_forecast/gold-flow", timeout=900)
        assert_deployment_ok(gold_result.state, logger, "gold", runtime)

        logger.info(
            "Completed Gold Flow Deployment",
            extra={
                "orchestrator_run_id": runtime.flow_run.id,
                "task_run_id": runtime.task_run.id if runtime.task_run else None,
                "deployment": "daily_dataset_forecast/gold-flow",
                "state": gold_result.state.type.value
            }
        )

        set_pipeline_status(
            pipeline_name=pipeline_name,
            status="success",
            run_id=run_id
        )
    except Exception as e:
        set_pipeline_status(
            pipeline_name=pipeline_name,
            status="failed",
            run_id=run_id,
            error=str(e)
        )

        raise

if __name__ == "__main__":
    orchestrator_flow()
