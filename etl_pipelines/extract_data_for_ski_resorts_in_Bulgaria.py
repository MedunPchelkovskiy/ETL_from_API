import json

from extract.extract_data_from_weather_APIs import extract_data_from_foreca_api, extract_data_from_accuweather_api, \
    get_from_meteoblue_api, extract_from_weatherbit_api, extract_data_from_tomorrow_api, \
    extract_data_from_openweathermap_api, extract_data_from_weatherapi_api, extract_data_from_open_meteo_api
from helpers.extraction_helpers.get_accuweather_location_id import get_accuweather_location_id_from_place_name
from helpers.extraction_helpers.get_foreca_location_id import get_foreca_location_id_from_place_name


def get_foreca_data(place_name: str):
    location_id = get_foreca_location_id_from_place_name(place_name)
    foreca_data = extract_data_from_foreca_api(location_id)
    return foreca_data


def get_accuweather_data(place_name: str):
    location_key = get_accuweather_location_id_from_place_name(place_name)
    accuweather_data = extract_data_from_accuweather_api(location_key)
    return accuweather_data


def get_meteoblue_data(place_name: str, country: str):
    meteoblue_data = get_from_meteoblue_api(place_name, country)
    return meteoblue_data


def get_weatherbit_data(postal_code: int, iso_country_code: str):
    weatherbit_data = extract_from_weatherbit_api(postal_code, iso_country_code)
    return weatherbit_data


def get_tomorrow_data(place_name: str):
    tomorrow_data = extract_data_from_tomorrow_api(place_name)
    return tomorrow_data


def get_openweathermap_data(place_name: str, iso_country_code: str):
    openweather_data = extract_data_from_openweathermap_api(place_name, iso_country_code)
    return openweather_data


def get_weatherapi_data(coordinates):
    weatherapi_data = extract_data_from_weatherapi_api(coordinates)
    return weatherapi_data


def get_open_meteo_data(coordinates: str):
    ope_meteo_data = extract_data_from_open_meteo_api(coordinates)
    return ope_meteo_data


if __name__ == "__main__":
    places_with_gps = {"Bansko":"41.77,23.43", "Pamporovo":"41.65,24.69", "Borovets":"42.27,23.60"}
    print(get_foreca_data("Bansko"))
    print(get_foreca_data("Pamporovo"))
    print(get_foreca_data("Borovets"))

    places = ["Bansko", "Pamporovo", "Borovets"]

    for place in places:
        try:
            result = get_accuweather_data(place)
            print(result)
        except ValueError as e:
            print(f"Skipping {place}: {e}")
    # print(get_accuweather_data("Bansko"))
    # print(get_accuweather_data("Pamporovo"))
    # print(get_accuweather_data("Borovets"))


    print(get_meteoblue_data("Bansko","Bulgaria"))
    print(get_meteoblue_data("Pamporovo","Bulgaria"))
    print(get_meteoblue_data("Borovets","Bulgaria"))

    print(get_weatherbit_data(2770,"BG"))
    print(get_weatherbit_data(4870,"BG"))
    print(get_weatherbit_data(2010,"BG"))

    print(get_tomorrow_data("Bansko"))
    print(get_tomorrow_data("Pamporovo"))
    print(get_tomorrow_data("Borovets"))

    print(get_openweathermap_data("Bansko", "BG"))
    print(get_openweathermap_data("Pamporovo", "BG"))
    print(get_openweathermap_data("Borovets", "BG"))



    print(get_weatherapi_data(places_with_gps["Bansko"]))
    print(get_weatherapi_data(places_with_gps["Pamporovo"]))
    print(get_weatherapi_data(places_with_gps["Borovets"]))




    result = get_open_meteo_data(places_with_gps["Bansko"])
    result = json.dumps(result, indent=4)
    print(result)
    result = get_open_meteo_data(places_with_gps["Pamporovo"])
    result = json.dumps(result, indent=4)
    print(result)
    result = get_open_meteo_data(places_with_gps["Borovets"])
    result = json.dumps(result, indent=4)
    print(result)

    # print(get_open_meteo_data(places["Bansko"]))
    # print(get_open_meteo_data(places["Pamporovo"]))
    # print(get_open_meteo_data(places["Borovets"]))