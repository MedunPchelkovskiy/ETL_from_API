from extract.extract_data_from_weather_APIs import extract_data_from_foreca_api, extract_data_from_accuweather_api, \
    get_from_meteoblue_api
from helpers.get_accuweather_location_id import get_accuweather_location_id_from_place_name
from helpers.get_foreca_location_id import get_foreca_location_id_from_place_name


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


if __name__ == "__main__":
    # print(get_foreca_data(place="Bansko"))
    # print(get_foreca_data(place="Pamporovo"))
    # print(get_foreca_data(place="Borovets"))
    # print(get_accuweather_data("Bansko"))
    # print(get_accuweather_data("Pamporovo"))
    # print(get_accuweather_data("Borovets"))
    print(get_meteoblue_data("Bansko","Bulgaria"))
    print(get_meteoblue_data("Pamporovo","Bulgaria"))
    print(get_meteoblue_data("Borovets","Bulgaria"))
