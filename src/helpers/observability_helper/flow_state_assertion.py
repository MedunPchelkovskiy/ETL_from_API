def assert_deployment_ok(state, logger, deployment, runtime):
    if state.is_failed() or state.is_crashed() or state.is_cancelled():
        logger.error(
            f"{deployment} failed",
            extra={
                "flow_run_id": runtime.flow_run.id,
                "deployment": deployment,
                "state_type": state.type.value,
                "state_name": state.name,
                "message": str(state.message) if state.message else None,
            }
        )
        raise RuntimeError(f"Stopping pipeline → {deployment} failed ({state.type.value})")