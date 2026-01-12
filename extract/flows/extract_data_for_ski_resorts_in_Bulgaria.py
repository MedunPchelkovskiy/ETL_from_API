from datetime import datetime

from decouple import config  # import configuration
from prefect import flow

from clients.datalake_client import fs_client
from helpers.extraction_helpers.api_tasks_mapper import api_tasks
from helpers.extraction_helpers.api_location_mapper import api_locations
from load.raw_data.tasks.load_raw_data import load_raw_api_data_to_azure_blob, load_raw_api_data_to_postgres_local


# INTERVAL = 3600

@flow(
    flow_run_name=lambda: f"extract_data_for_ski_resorts_in_Bulgaria - {datetime.now().strftime('%d%m%Y-%H%M%S')}"   # Lambda give dynamically timestamp on every flow execution
)
def weather_flow_run(debug: bool = False):
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")

    for api_name, locations in api_locations.items():
        api_task = api_tasks[api_name]

        for label, payload in locations:
            # Call API
            # data = api_task(payload)
            if isinstance(payload, str):
                payload = (payload,)
            state = api_task.submit(*payload,return_state=True)

            if state.is_failed():
                # 🔴 логваш, метрики, алерт и продължаваш
                print(f"❌ Failed for {api_name} - {label}")
                continue

            data = state.result()

            # Build human-readable folder/file names
            folder_name = f"{date_str}_{api_name}_{label}"
            file_name = f"{hour_str}.json"

            # Upload JSON to Azure
            load_raw_api_data_to_azure_blob(fs_client, config("BASE_DIR"), folder_name, file_name, data["data"])
            # Upload JSON local to postgres
            load_raw_api_data_to_postgres_local(data, label)
    print(f"Running flow at {datetime.now()}")


if __name__ == "__main__":
    weather_flow_run()
    # next_run = time.time()
    # while True:
    #     weather_flow_run()
    #     next_run += INTERVAL
    #     sleep_time = next_run - time.time()
    #     time.sleep(sleep_time)
