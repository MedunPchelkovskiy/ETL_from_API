from extract.extract_data_from_weather_APIs import extract_data_from_meteoblue_api, \
                                                   extract_data_from_sinoptik_api, \
                                                   extract_data_from_accuweather_api
from load.from_weather_APIs import load_data
from transform.accuweather_data_transformation import accuweather_transformation
from transform.meteoblue_data_transformation import meteoblue_transformation
from transform.sinoptik_data_transformation import sinoptik_transformation

if __name__ == "__main__":
    meteoblue_data = extract_data_from_meteoblue_api("https://www.meteoblue.com/en/weather/today/veliko-tarnovo_bulgaria_725993")
    sinoptik_data = extract_data_from_sinoptik_api("https://www.sinoptik.bg/veliko-turnovo-bulgaria-100725993")
    accuweather_data = extract_data_from_accuweather_api("https://www.accuweather.com/bg/bg/veliko-tarnovo/46650/current-weather/46650")


    transformed_accuweather_data = accuweather_transformation(accuweather_data)
    transformed_sinoptik_data = sinoptik_transformation(sinoptik_data)
    transformed_meteoblue_data = meteoblue_transformation(meteoblue_data)


    load_data(transformed_meteoblue_data, "meteoblue_data")
    load_data(transformed_sinoptik_data, "sinoptik_data")
    load_data(transformed_accuweather_data, "accuweather_data")

    print(meteoblue_data)
    print(sinoptik_data)
    print(transformed_accuweather_data)
