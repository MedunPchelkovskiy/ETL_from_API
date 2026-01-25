api_paths = {
    "openweather": {
        "dt": "dt_txt",
        "temp": "main.temp",
        "humidity": "main.humidity",
        "wind_speed": "wind.speed",
        "weather_main": "weather.0.main",
        "weather_desc": "weather.0.description"
    },
    "tomorrow": {
        ...
    }
}
    # TODO: да се проверят стойностите на първия мапър за съответствие с апи респонса/
    #  и да се допълни и за дугите апи-та











# # Map API names to parsing bronze data functions
# from src.workers.silver.parsers_bronze_data import parse_accuweather_data, parse_meteoblue_data, parse_tomorrow_data, \
#     parse_openweathermap_data, parse_weatherapi_data, parse_open_meteo_data, parse_foreca_data
#
# api_parsers = {
#     "foreca": parse_foreca_data,
#     "accuweather": parse_accuweather_data,
#     "meteoblue": parse_meteoblue_data,
#     "tomorrow": parse_tomorrow_data,
#     "openweathermap": parse_openweathermap_data,
#     "weatherapi": parse_weatherapi_data,
#     "open_meteo": parse_open_meteo_data,
# }
