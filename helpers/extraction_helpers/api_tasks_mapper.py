# Map API names to their functions
from extract.tasks.weather_tasks import get_accuweather_data, get_meteoblue_data,    \
    get_tomorrow_data, get_openweathermap_data, get_weatherapi_data, get_open_meteo_data

api_tasks = {
    "accuweather": get_accuweather_data,
    "meteoblue": get_meteoblue_data,
    "tomorrow": get_tomorrow_data,
    "openweathermap": get_openweathermap_data,
    "weatherapi": get_weatherapi_data,
    "open_meteo": get_open_meteo_data,
}



# api_tasks = {
#     "accuweather": get_accuweather_data,
#     "meteoblue": lambda payload: get_meteoblue_data(payload[0], payload[1]),
#     "weatherbit": lambda payload: get_weatherbit_data(payload[0], payload[1]),
#     "tomorrow": get_tomorrow_data,
#     "openweathermap": lambda payload: get_openweathermap_data(payload[0], payload[1]),
#     "weatherapi": lambda payload: get_weatherapi_data(payload[0], payload[1]),
#     "open_meteo": lambda payload: get_open_meteo_data(payload[0], payload[1]),
# }
