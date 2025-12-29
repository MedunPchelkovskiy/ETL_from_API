from datetime import datetime

from decouple import config  # import configuration

from authentication.azure_auth import get_datalake_client
from extract.extract_data_from_weather_APIs import extract_data_from_foreca_api, extract_data_from_accuweather_api, \
    get_from_meteoblue_api, extract_from_weatherbit_api, extract_data_from_tomorrow_api, \
    extract_data_from_openweathermap_api, extract_data_from_weatherapi_api, extract_data_from_open_meteo_api
from helpers.extraction_helpers.get_accuweather_location_id import get_accuweather_location_id_from_place_name
from helpers.extraction_helpers.get_foreca_location_id import get_foreca_location_id_from_place_name
from load.load_extracted_data_from_weather_APIs import upload_json


# --------------------------
# Separate functions for each API
# --------------------------
# def fetch_api1():
#     return {"api": "api1", "data": "example1"}
#
# def fetch_api2():
#     return {"api": "api2", "data": "example2"}

# ... Add functions for remaining APIs

def get_foreca_data(place_name: str):
    api = "foreca_api"
    location_id = get_foreca_location_id_from_place_name(place_name)
    foreca_data = extract_data_from_foreca_api(location_id)
    return {"api": api, "data": foreca_data}


def get_accuweather_data(place_name: str):
    api = "accuweather_api"
    location_key = get_accuweather_location_id_from_place_name(place_name)
    accuweather_data = extract_data_from_accuweather_api(location_key)
    return {"api": api, "data": accuweather_data}


def get_meteoblue_data(place_name: str, country: str):
    api = "meteoblue_api"
    meteoblue_data = get_from_meteoblue_api(place_name, country)
    return {"api": api, "data": meteoblue_data}


def get_weatherbit_data(postal_code: int, iso_country_code: str):
    api = "weatherbit_api"
    weatherbit_data = extract_from_weatherbit_api(postal_code, iso_country_code)
    return {"api": api, "data": weatherbit_data}


def get_tomorrow_data(place_name: str):
    api = "tomorrow_api"
    tomorrow_data = extract_data_from_tomorrow_api(place_name)
    return {"api": api, "data": tomorrow_data}


def get_openweathermap_data(place_name: str, iso_country_code: str):
    api = "openweathermap_api"
    openweather_data = extract_data_from_openweathermap_api(place_name, iso_country_code)
    return {"api": api, "data": openweather_data}


def get_weatherapi_data(lat, lan):
    api = "weatherapi_api"
    weatherapi_data = extract_data_from_weatherapi_api(lat, lan)
    return {"api": api, "data": weatherapi_data}


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
        ("Bansko", ("Bansko","Bulgaria")),
        ("Pamporovo", ("Pamporovo","Bulgaria")),
        ("Borovets", ("Borovets","Bulgaria")),
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
        ("Bansko", (41.77,23.43)),
        ("Pamporovo", (41.65,24.69)),
        ("Borovets", (42.27,23.60)),
    ],
    "open_meteo": [
        ("Bansko", (41.77,23.43)),
        ("Pamporovo", (41.65,24.69)),
        ("Borovets", (42.27,23.60)),
    ],
}

if __name__ == "__main__":

    for api_name, locations in api_locations.items():
        api_func = api_functions[api_name]

        for label, payload in locations:
            # Call API
            data = api_func(payload)

            # Build human-readable folder/file names
            folder_name = f"{date_str}_{api_name}_{label}"
            file_name = f"{hour_str}.json"

            # Upload JSON
            upload_json(fs_client, config("BASE_DIR"), folder_name, file_name, data["data"])

    # for api_name, locations in api_locations.items():
    #     for place_name in locations:
    #         data = api_functions[api_name](place_name)
    #         folder_name = f"{date_str}_{api_name}_{place_name}"  # unique folder per API + location
    #         file_name = f"{hour_str}.json"  # or include minutes if needed
    #         upload_json(fs_client, config("BASE_DIR"), folder_name, file_name, data["data"])

    # for api_name, api_func in api_functions.items():
    #     try:
    #         data = api_func()
    #         folder_name = f"{date_str}_{api_name}"  # Hybrid folder structure
    #         file_name = f"{hour_str}.json"
    #         upload_json(fs_client, config.BASE_DIR, folder_name, file_name, data)
    #     except Exception as e:
    #         print(f"❌ Failed to fetch/upload data for {api_name}: {e}")

# def get_foreca_data(place_name: str):
#     location_id = get_foreca_location_id_from_place_name(place_name)
#     foreca_data = extract_data_from_foreca_api(location_id)
#     return foreca_data
#
#
# def get_accuweather_data(place_name: str):
#     location_key = get_accuweather_location_id_from_place_name(place_name)
#     accuweather_data = extract_data_from_accuweather_api(location_key)
#     return accuweather_data
#
#
# def get_meteoblue_data(place_name: str, country: str):
#     meteoblue_data = get_from_meteoblue_api(place_name, country)
#     return meteoblue_data
#
#
# def get_weatherbit_data(postal_code: int, iso_country_code: str):
#     weatherbit_data = extract_from_weatherbit_api(postal_code, iso_country_code)
#     return weatherbit_data
#
#
# def get_tomorrow_data(place_name: str):
#     tomorrow_data = extract_data_from_tomorrow_api(place_name)
#     return tomorrow_data
#
#
# def get_openweathermap_data(place_name: str, iso_country_code: str):
#     openweather_data = extract_data_from_openweathermap_api(place_name, iso_country_code)
#     return openweather_data
#
#
# def get_weatherapi_data(coordinates):
#     weatherapi_data = extract_data_from_weatherapi_api(coordinates)
#     return weatherapi_data
#
#
# def get_open_meteo_data(coordinates: str):
#     ope_meteo_data = extract_data_from_open_meteo_api(coordinates)
#     return ope_meteo_data


# if __name__ == "__main__":
# places_with_gps = {"Bansko":"41.77,23.43", "Pamporovo":"41.65,24.69", "Borovets":"42.27,23.60"}
# print(get_foreca_data("Bansko"))
# print(get_foreca_data("Pamporovo"))
# print(get_foreca_data("Borovets"))
#
# places = ["Bansko", "Pamporovo", "Borovets"]
#
# for place in places:
#     try:
#         result = get_accuweather_data(place)
#         print(result)
#     except ValueError as e:
#         print(f"Skipping {place}: {e}")
# print(get_accuweather_data("Bansko"))
# print(get_accuweather_data("Pamporovo"))
# print(get_accuweather_data("Borovets"))

#
# print(get_meteoblue_data("Bansko","Bulgaria"))
# print(get_meteoblue_data("Pamporovo","Bulgaria"))
# print(get_meteoblue_data("Borovets","Bulgaria"))
#
# print(get_weatherbit_data(2770,"BG"))
# print(get_weatherbit_data(4870,"BG"))
# print(get_weatherbit_data(2010,"BG"))
#
# print(get_tomorrow_data("Bansko"))
# print(get_tomorrow_data("Pamporovo"))
# print(get_tomorrow_data("Borovets"))
#
# print(get_openweathermap_data("Bansko", "BG"))
# print(get_openweathermap_data("Pamporovo", "BG"))
# print(get_openweathermap_data("Borovets", "BG"))
#
#
#
# print(get_weatherapi_data(places_with_gps["Bansko"]))
# print(get_weatherapi_data(places_with_gps["Pamporovo"]))
# print(get_weatherapi_data(places_with_gps["Borovets"]))
#
#
#
#
# result = get_open_meteo_data(places_with_gps["Bansko"])
# result = json.dumps(result, indent=4)
# print(result)
# result = get_open_meteo_data(places_with_gps["Pamporovo"])
# result = json.dumps(result, indent=4)
# print(result)
# result = get_open_meteo_data(places_with_gps["Borovets"])
# result = json.dumps(result, indent=4)
# print(result)

# print(get_open_meteo_data(places["Bansko"]))
# print(get_open_meteo_data(places["Pamporovo"]))
# print(get_open_meteo_data(places["Borovets"]))
