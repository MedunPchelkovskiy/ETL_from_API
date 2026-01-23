import subprocess
from prefect import flow, get_run_logger

@flow
def master_flow(debug: bool = False):
    logger = get_run_logger()

    # --- Trigger first flow deployment ---
    logger.info("Triggering first flow (weather-flow-run)...")
    subprocess.run([
        "prefect", "deployment", "run", "weather-flow-run", "-p", f"debug={debug}"
    ], check=True)
    logger.info("First flow triggered!")

    # --- Trigger second flow deployment ---
    logger.info("Triggering second flow (second-flow-dev)...")
    subprocess.run([
        "prefect", "deployment", "run", "second-flow-dev", "-p", f"debug={debug}"
    ], check=True)
    logger.info("Second flow triggered!")
