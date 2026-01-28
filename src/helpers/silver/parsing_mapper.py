from src.workers.silver.parsing_workers import parse_open_meteo_daily, parse_foreca_daily, \
    parse_accuweather_daily, parse_weatherapi_daily, parse_openweathermap_3h, parse_tomorrowio_daily, \
    parse_weatherbit_daily, parse_meteoblue_basic_day

api_data_parsers = {
    "open_meteo": parse_open_meteo_daily,           # Open-Meteo daily forecast
    "foreca": parse_foreca_daily,                    # Foreca daily forecast
    "accuweather": parse_accuweather_daily,         # AccuWeather 5-day
    "meteoblue": parse_meteoblue_basic_day,             # Meteoblue daily
    "weatherbit": parse_weatherbit_daily,           # Weatherbit daily
    "tomorrow": parse_tomorrowio_daily,             # Tomorrow.io daily
    "openweathermap": parse_openweathermap_3h,      # OpenWeatherMap 3-hour forecast
    "weatherapi": parse_weatherapi_daily,           # WeatherAPI.com daily
}
