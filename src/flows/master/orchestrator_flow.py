from prefect import flow, runtime
from prefect.deployments import run_deployment

from src.helpers.logging_helper.combine_loggers_helper import get_logger


@flow(name="OrchestratorFlow")
def orchestrator_flow():
    """
    Orchestrates two Prefect 3 deployments sequentially with structured logging.
    Logs include flow_run_id and task_run_id like your example.
    """

    # Get logger and run context
    logger = get_logger()

    # --- Run first deployment ---
    logger.info(
        "Starting First Flow Deployment...",
        extra={
            "flow_run_id": runtime.flow_run.id,
            "task_run_id": runtime.task_run.id if runtime.task_run else None,
            "deployment": "weather-flow-run/local-dev"
        }
    )
    first_result = run_deployment("weather-flow-run/Bronze")
    logger.info(
        "Completed First Flow Deployment",
        extra={
            "flow_run_id": runtime.flow_run.id,
            "task_run_id": runtime.task_run.id if runtime.task_run else None,
            "deployment": "weather-flow-run/local-dev",
            "state": first_result.state.type.value
        }
    )

    # --- Run second deployment ---
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


if __name__ == "__main__":
    orchestrator_flow()
