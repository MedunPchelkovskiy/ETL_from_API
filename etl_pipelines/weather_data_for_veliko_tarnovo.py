from extract.extract_data_from_weather_APIs import extract_data_from_meteoblue_api, \
                                                   extract_data_from_sinoptik_api, \
                                                   extract_data_from_accuweather_api
from load.from_weather_APIs import load_data
from transform.accuweather_data_transformation import accuweather_transformation
from transform.sinoptik_data_transformation import sinoptik_transformation

if __name__ == "__main__":
    meteo_blue_data = extract_data_from_meteoblue_api("veliko tarnovo")
    sinoptik_data = extract_data_from_sinoptik_api()
    accuweather_data = extract_data_from_accuweather_api()


    transformed_accuweather_data = accuweather_transformation(accuweather_data)
    transformed_sinoptik_data = sinoptik_transformation(sinoptik_data)


    load_data(meteo_blue_data, "meteoblue_data")
    load_data(transformed_sinoptik_data, "sinoptik_data")
    load_data(transformed_accuweather_data, "accuweather_data")

    print(meteo_blue_data)
    print(sinoptik_data)
    print(transformed_accuweather_data)
