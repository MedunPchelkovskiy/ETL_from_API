from extract.extract_data_from_weather_APIs import extract_data_from_meteoblue_api

if __name__ == "__main__":
    meteo_blue_data = extract_data_from_meteoblue_api("veliko tarnovo")
    print(meteo_blue_data)
