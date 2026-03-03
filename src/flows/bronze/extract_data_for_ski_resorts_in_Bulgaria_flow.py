from datetime import datetime

import pendulum
from decouple import config  # import configuration
from prefect import flow
from prefect.client import get_client

from logging_config import setup_logging
from src.clients.datalake_client import fs_client
from src.helpers.bronze.api_location_mapper import api_locations
from src.helpers.bronze.extract_tasks_mapper import api_tasks
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.observability_helper.metrics_server import start_metrics_server
from src.tasks.bronze.load_raw_weather_data_tasks import load_raw_api_data_to_azure_blob, \
    load_raw_api_data_to_postgres_local


# INTERVAL = 3600

@flow(
    flow_run_name=lambda: f"extract_data_for_ski_resorts_in_Bulgaria - {pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss")}",
    # Lambda give dynamically timestamp on every flow execution
)
def weather_flow_run(debug: bool = False):
    now = pendulum.now("UTC")
    start_metrics_server()
    date_str = now.format("YYYY-MM-DD")
    hour_str = now.format("HH")
    setup_logging()
    logger = get_logger()

    logger.info("Starting flow extract bronze data")

    for api_name, locations in api_locations.items():
        api_task = api_tasks[api_name]

        for label, payload in locations:
            # Call API
            # data = api_task(payload)
            call_args = (payload,) if isinstance(payload, str) else payload
            state = api_task.submit(*call_args, return_state=True)

            if state.is_failed():
                # 🔴 логваш, метрики, алерт и продължаваш
                logger.warning(f"❌ Failed for {api_name} - {label}")
                continue

            data = state.result()

            # Build human-readable folder/file names
            folder_name = f"{date_str}_{api_name}_{label}"
            file_name = f"{hour_str}.json"

            # Upload JSON to Azure
            load_raw_api_data_to_azure_blob(fs_client, config("BASE_DIR_RAW"), folder_name, file_name, data["data"])
            # Upload JSON local to postgres
            load_raw_api_data_to_postgres_local(data, label)
    logger.info(f"Running flow at {now}")


def main_flow():
    start_metrics_server()
    weather_flow_run()


if __name__ == "__main__":
    main_flow()
