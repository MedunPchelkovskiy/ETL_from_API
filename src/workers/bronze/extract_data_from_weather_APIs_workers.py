import requests
from decouple import config

from src.helpers.bronze.get_meteoblue_location import get_lat_lon_from_place_name
from src.helpers.bronze.get_openweathermap_location import get_owm_lat_lon_from_place_name_iso_country_code
from src.helpers.logging_helper.combine_loggers_helper import get_logger
from logging_config import setup_logging

setup_logging()
logger = get_logger()


def extract_data_from_foreca_api(location_id: int):
    url = f"https://pfa.foreca.com/api/v1/forecast/daily/{location_id}?lang=en&token={config("FORECA_API_KEY")}"
    response = requests.get(url)
    return response.json()


def extract_data_from_accuweather_api(location_key: int):
    url = f"https://dataservice.accuweather.com/forecasts/v1/daily/5day/{location_key}?metric=true"
    headers = {"Authorization": f"Bearer {config('ACCUWEATHER_API_KEY')}"}
    response = requests.get(url, headers=headers)
    return response.json()


def get_from_meteoblue_api(place_name, country):
    lat, lon = get_lat_lon_from_place_name(place_name, country)
    url = f"https://my.meteoblue.com/packages/basic-day?lat={lat}&lon={lon}&forecast_days=5&apikey={config('METEOBLUE_API_KEY')}"
    response = requests.get(url)
    return response.json()


# def extract_from_weatherbit_api(postal_code, iso_country_code):
#     """
#     Returns Weatherbit forecast JSON for a location.
#     Skips if lat/lon lookup fails or API rate limit (429) occurs.
#     """
#     lat_lon = get_lat_lon_from_postal_code_iso_country_code(postal_code, iso_country_code)
#     if lat_lon is None:
#         print(f"[{datetime.now()}] Skipping {postal_code}/{iso_country_code} due to failed lat/lon lookup")
#         return None
#
#     lat, lon = lat_lon
#
#     try:
#         url = (
#             f"https://api.weatherbit.io/v2.0/forecast/daily?"
#             f"lat={lat}&lon={lon}&key={config('WEATHERBIT_API_KEY')}&hours=72"
#         )
#         response = requests.get(url)
#
#         if response.status_code == 429:
#             print(f"[{datetime.now()}] Rate limit hit for {postal_code}/{iso_country_code}. Skipping.")
#             return None
#
#         if response.status_code != 200:
#             print(f"[{datetime.now()}] Request failed: {response.status_code} - {response.text}. Skipping.")
#             return None
#
#         return response.json()
#
#     except (requests.RequestException, requests.JSONDecodeError) as e:
#         print(f"[{datetime.now()}] Weatherbit request error for {postal_code}/{iso_country_code}: {e}. Skipping.")
#         return None


def extract_data_from_tomorrow_api(place_name):
    url = f"https://api.tomorrow.io/v4/weather/forecast?location={place_name}k&timesteps=daily:'7d'&apikey={config('TOMMOROW_API_KEY')}"

    headers = {
        "accept": "application/json",
        "accept-encoding": "deflate, gzip, br"
    }

    response = requests.get(url, headers=headers)
    return response.json()


def extract_data_from_openweathermap_api(place_name, iso_country_code):
    lat, lon = get_owm_lat_lon_from_place_name_iso_country_code(place_name, iso_country_code)
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&exclude=current,minutely,daily,alerts&units=metric&lang=en&appid={config('OPENWEATHERMAP_API_KEY')}"
    response = requests.get(url)
    return response.json()


def extract_data_from_weatherapi_api(lat, lon):
    url = f"http://api.weatherapi.com/v1/forecast.json?key={config('WEATHERAPI_API_KEY')}&q={lat},{lon}&days=7&aqi=no&alerts=no&pollen=no&tides=no"
    response = requests.get(url)
    return response.json()


def extract_data_from_open_meteo_api(lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,windspeed_10m_max,cloudcover_mean,weathercode&forecast_days=7&timezone=UTC"
    response = requests.get(url)
    return response.json()
