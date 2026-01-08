from datetime import datetime

from decouple import config  # import configuration
from prefect import flow, task

from authentication.azure_auth import get_datalake_client
from extract.extract_data_from_weather_APIs import extract_data_from_foreca_api, extract_data_from_accuweather_api, \
    get_from_meteoblue_api, extract_from_weatherbit_api, extract_data_from_tomorrow_api, \
    extract_data_from_openweathermap_api, extract_data_from_weatherapi_api, extract_data_from_open_meteo_api
from helpers.extraction_helpers.get_accuweather_location_id import get_accuweather_location_id_from_place_name
from helpers.extraction_helpers.get_foreca_location_id import get_foreca_location_id_from_place_name
from load.load_raw_data_from_weather_APIs_to_Azure import upload_json
from load.load_raw_data_from_weather_APIs_to_local_postgres import load_raw_api_data_to_postgres

# --------------------------
# Separate functions for each API
# --------------------------
# def fetch_api1():
#     return {"api": "api1", "data": "example1"}
#
# def fetch_api2():
#     return {"api": "api2", "data": "example2"}

# ... Add functions for remaining APIs

INTERVAL = 3600


# @task(task_run_name=lambda place_name: f"foreca-{place_name}")
@task(task_run_name=lambda **kwargs: f"foreca-{kwargs['place_name']}")
def get_foreca_data(place_name: str):
    api = "foreca_api"
    location_id = get_foreca_location_id_from_place_name(place_name)
    foreca_data = extract_data_from_foreca_api(location_id)
    return {"api": api, "data": foreca_data}


# @task(task_run_name=lambda place_name: f"accuweather-{place_name}")
@task(task_run_name=lambda **kwargs: f"accuweather-{kwargs['place_name']}")
def get_accuweather_data(place_name: str):
    api = "accuweather_api"
    location_key = get_accuweather_location_id_from_place_name(place_name)
    accuweather_data = extract_data_from_accuweather_api(location_key)
    return {"api": api, "data": accuweather_data}


# @task(task_run_name=lambda place_name: f"meteoblue-{place_name}")
@task(task_run_name=lambda **kwargs: f"meteoblue-{kwargs['place_name']}")
def get_meteoblue_data(place_name: str, country: str):
    api = "meteoblue_api"
    meteoblue_data = get_from_meteoblue_api(place_name, country)
    return {"api": api, "data": meteoblue_data}


# @task(task_run_name=lambda place_name: f"weatherbit-{place_name}")
@task(task_run_name=lambda **kwargs: f"weatherbit-{kwargs['place_name']}")
def get_weatherbit_data(postal_code: int, iso_country_code: str):
    api = "weatherbit_api"
    weatherbit_data = extract_from_weatherbit_api(postal_code, iso_country_code)
    return {"api": api, "data": weatherbit_data}


# @task(task_run_name=lambda place_name: f"tommorow-{place_name}")
@task(task_run_name=lambda **kwargs: f"tommorow-{kwargs['place_name']}")
def get_tomorrow_data(place_name: str):
    api = "tomorrow_api"
    tomorrow_data = extract_data_from_tomorrow_api(place_name)
    return {"api": api, "data": tomorrow_data}


# @task(task_run_name=lambda place_name: f"openweathermap-{place_name}")
@task(task_run_name=lambda **kwargs: f"openweathermap-{kwargs['place_name']}")
def get_openweathermap_data(place_name: str, iso_country_code: str):
    api = "openweathermap_api"
    openweather_data = extract_data_from_openweathermap_api(place_name, iso_country_code)
    return {"api": api, "data": openweather_data}


# @task(task_run_name=lambda place_name: f"weatherapi-{place_name}")
@task(task_run_name=lambda **kwargs: f"weatherapi-{kwargs['place_name']}")
def get_weatherapi_data(lat, lan):
    api = "weatherapi_api"
    weatherapi_data = extract_data_from_weatherapi_api(lat, lan)
    return {"api": api, "data": weatherapi_data}


# @task(task_run_name=lambda place_name: f"open-meteo-{place_name}")
@task(task_run_name=lambda **kwargs: f"open-meteo-{kwargs['place_name']}")
def get_open_meteo_data(lat, lan):
    api = "open_meteo_api"
    ope_meteo_data = extract_data_from_open_meteo_api(lat, lan)
    return {"api": api, "data": ope_meteo_data}


# Map API names to their functions
api_functions = {
    "accuweather": get_accuweather_data,
    "meteoblue": lambda payload: get_meteoblue_data(payload[0], payload[1]),
    "weatherbit": lambda payload: get_weatherbit_data(payload[0], payload[1]),
    "tomorrow": get_tomorrow_data,
    "openweathermap": lambda payload: get_openweathermap_data(payload[0], payload[1]),
    "weatherapi": lambda payload: get_weatherapi_data(payload[0], payload[1]),
    "open_meteo": lambda payload: get_open_meteo_data(payload[0], payload[1]),
}

# --------------------------
# Create Data Lake client
# --------------------------
fs_client = get_datalake_client(
    tenant_id=config("TENANT_ID"),
    client_id=config("CLIENT_ID"),
    client_secret=config("CLIENT_SECRET"),
    account_url=config("ACCOUNT_URL"),
    file_system=config("FILE_SYSTEM")
)

# --------------------------
# Upload loop
# --------------------------
now = datetime.now()
date_str = now.strftime("%Y-%m-%d")
hour_str = now.strftime("%H")

api_locations = {
    "accuweather": [
        ("Bansko", "Bansko"),
        ("Pamporovo", "Pamporovo"),
        ("Borovets", "Borovets"),
    ],
    "meteoblue": [
        ("Bansko", ("Bansko", "Bulgaria")),
        ("Pamporovo", ("Pamporovo", "Bulgaria")),
        ("Borovets", ("Borovets", "Bulgaria")),
    ],
    "weatherbit": [
        ("Bansko", (2770, "BG")),
        ("Pamporovo", (4870, "BG")),
        ("Borovets", (2010, "BG")),
    ],
    "tomorrow": [
        ("Bansko", "Bansko"),
        ("Pamporovo", "Pamporovo"),
        ("Borovets", "Borovets"),
    ],
    "openweathermap": [
        ("Bansko_BG", ("Bansko", "BG")),
        ("Pamporovo_BG", ("Pamporovo", "BG")),
        ("Borovets_BG", ("Borovets", "BG")),
    ],
    "weatherapi": [
        ("Bansko", (41.77, 23.43)),
        ("Pamporovo", (41.65, 24.69)),
        ("Borovets", (42.27, 23.60)),
    ],
    "open_meteo": [
        ("Bansko", (41.77, 23.43)),
        ("Pamporovo", (41.65, 24.69)),
        ("Borovets", (42.27, 23.60)),
    ],
}


@task(task_run_name=lambda **kwargs: f"upload-{kwargs['folder_name']}")
def load_raw_api_data_to_azure_blob(fs_client, base_dir, folder_name, file_name, data):
    upload_json(fs_client, base_dir, folder_name, file_name, data)


@task(task_run_name=lambda **kwargs: f"postgres-{kwargs['data']['api']}")
def load_raw_api_data_to_postgres_local(data):
    load_raw_api_data_to_postgres(data)


@flow(flow_run_name="extract_data_for_ski_resorts_in_Bulgaria")
def flow_run():
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
    flow_run()
    # next_run = time.time()
    # while True:
    #     flow_run()
    #     next_run += INTERVAL
    #     sleep_time = next_run - time.time()
    #     time.sleep(sleep_time)
