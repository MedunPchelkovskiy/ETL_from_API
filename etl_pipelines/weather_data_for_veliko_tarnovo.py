from extract.extract_data_from_weather_APIs import extract_data_from_meteoblue_api
from load.from_meteoblue_for_veliko_tarnovo import load_data

if __name__ == "__main__":
    meteo_blue_data = extract_data_from_meteoblue_api("veliko tarnovo")
    load_data(meteo_blue_data, "meteoblue_data")
    print(meteo_blue_data)
