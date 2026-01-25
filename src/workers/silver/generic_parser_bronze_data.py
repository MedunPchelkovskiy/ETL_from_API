import pandas as pd


def parse_api_df(df: pd.DataFrame, api_name: str, api_paths: dict):
    paths = api_paths.get(api_name)
    if not paths:
        return None

    parsed_data = {}
    for key, col in paths.items():
        if col in df.columns:
            parsed_data[key] = df[col]
        else:
            parsed_data[key] = None
    return pd.DataFrame(parsed_data)

# import pandas as pd
#
# def parse_foreca_data(raw: pd.DataFrame) -> dict:
#     """Extract fields from OpenWeather JSON payload into standard schema."""
#     return {
#         "dt": raw.get("dt_txt"),
#         "temp": raw.get("main", {}).get("temp"),
#         "humidity": raw.get("main", {}).get("humidity"),
#         "wind_speed": raw.get("wind", {}).get("speed"),
#         "weather_main": raw.get("weather", [{}])[0].get("main"),
#     }
#
#
# def parse_accuweather_data(raw: pd.DataFrame) -> dict:
#     """Extract fields from OpenWeather JSON payload into standard schema."""
#     return {
#         "dt": raw.get("dt_txt"),
#         "temp": raw.get("main", {}).get("temp"),
#         "humidity": raw.get("main", {}).get("humidity"),
#         "wind_speed": raw.get("wind", {}).get("speed"),
#         "weather_main": raw.get("weather", [{}])[0].get("main"),
#     }
#
#
# def parse_meteoblue_data(raw: pd.DataFrame) -> dict:
#     """Extract fields from OpenWeather JSON payload into standard schema."""
#     return {
#         "dt": raw.get("dt_txt"),
#         "temp": raw.get("main", {}).get("temp"),
#         "humidity": raw.get("main", {}).get("humidity"),
#         "wind_speed": raw.get("wind", {}).get("speed"),
#         "weather_main": raw.get("weather", [{}])[0].get("main"),
#     }
#
#
# def parse_tomorrow_data(raw: pd.DataFrame) -> dict:
#     """Extract fields from OpenWeather JSON payload into standard schema."""
#     return {
#         "dt": raw.get("dt_txt"),
#         "temp": raw.get("main", {}).get("temp"),
#         "humidity": raw.get("main", {}).get("humidity"),
#         "wind_speed": raw.get("wind", {}).get("speed"),
#         "weather_main": raw.get("weather", [{}])[0].get("main"),
#     }
#
#
# def parse_openweathermap_data(raw: pd.DataFrame) -> dict:
#     """Extract fields from OpenWeather JSON payload into standard schema."""
#     return {
#         "dt": raw.get("dt_txt"),
#         "temp": raw.get("main", {}).get("temp"),
#         "humidity": raw.get("main", {}).get("humidity"),
#         "wind_speed": raw.get("wind", {}).get("speed"),
#         "weather_main": raw.get("weather", [{}])[0].get("main"),
#     }
#
#
# def parse_weatherapi_data(raw: pd.DataFrame) -> dict:
#     """Extract fields from OpenWeather JSON payload into standard schema."""
#     return {
#         "dt": raw.get("dt_txt"),
#         "temp": raw.get("main", {}).get("temp"),
#         "humidity": raw.get("main", {}).get("humidity"),
#         "wind_speed": raw.get("wind", {}).get("speed"),
#         "weather_main": raw.get("weather", [{}])[0].get("main"),
#     }
#
#
# def parse_open_meteo_data(raw: pd.DataFrame) -> dict:
#     """Extract fields from OpenWeather JSON payload into standard schema."""
#     return {
#         "dt": raw.get("dt_txt"),
#         "temp": raw.get("main", {}).get("temp"),
#         "humidity": raw.get("main", {}).get("humidity"),
#         "wind_speed": raw.get("wind", {}).get("speed"),
#         "weather_main": raw.get("weather", [{}])[0].get("main"),
#     }
