from datetime import datetime

from decouple import config  # import configuration
from prefect import flow

from clients.datalake_client import fs_client
from helpers.extraction_helpers.api_functions_mapper import api_functions
from helpers.extraction_helpers.api_location_mapper import api_locations
from load.raw_data.tasks.load_raw_data import load_raw_api_data_to_azure_blob, load_raw_api_data_to_postgres_local


# INTERVAL = 3600


@flow(flow_run_name="extract_data_for_ski_resorts_in_Bulgaria")
def weather_flow_run():
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")

    for api_name, locations in api_locations.items():
        api_func = api_functions[api_name]

        for label, payload in locations:
            # Call API
            data = api_func(payload)

            # Build human-readable folder/file names
            folder_name = f"{date_str}_{api_name}_{label}"
            file_name = f"{hour_str}.json"

            # Upload JSON to Azure
            load_raw_api_data_to_azure_blob(fs_client, config("BASE_DIR"), folder_name, file_name, data["data"])
            # Upload JSON local to postgres
            load_raw_api_data_to_postgres_local(data)
    print(f"Running flow at {datetime.now()}")


if __name__ == "__main__":
    weather_flow_run()
    # next_run = time.time()
    # while True:
    #     weather_flow_run()
    #     next_run += INTERVAL
    #     sleep_time = next_run - time.time()
    #     time.sleep(sleep_time)
