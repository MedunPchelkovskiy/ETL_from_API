import time
from datetime import datetime

import requests
from decouple import config

from helpers.extraction_helpers.get_meteoblue_location import get_lat_lon_from_place_name
from helpers.extraction_helpers.get_openweathermap_location import get_owm_lat_lon_from_place_name_iso_country_code
from helpers.extraction_helpers.get_weatherbit_location import get_lat_lon_from_postal_code_iso_country_code


def extract_data_from_foreca_api(location_id: int, max_retries: int = 5):
    """Function that returns data from foreca api. Max_retries is set to 5. Api have request limit and
     if you exceed the allowed requests per second, the API responds with a rate limit error instead of valid data."""

    url = f"https://pfa.foreca.com/api/v1/forecast/daily/{location_id}?lang=en&token={config("FORECA_API_KEY")}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url)

            if response.status_code == 429:
                print("Rate limit reached. Retrying in 2 seconds...")
                time.sleep(2)
                continue

            response.raise_for_status()  # raise for other HTTP errors
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}. Retrying in 2 seconds...")
            time.sleep(2)

    print(f"Failed to fetch data for place_id {location_id} after {max_retries} attempts.")
    return None


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


def extract_from_weatherbit_api(postal_code, iso_country_code):
    """
    Returns Weatherbit forecast JSON for a location.
    Skips if lat/lon lookup fails or API rate limit (429) occurs.
    """
    lat_lon = get_lat_lon_from_postal_code_iso_country_code(postal_code, iso_country_code)
    if lat_lon is None:
        print(f"[{datetime.now()}] Skipping {postal_code}/{iso_country_code} due to failed lat/lon lookup")
        return None

    lat, lon = lat_lon

    try:
        url = (
            f"https://api.weatherbit.io/v2.0/forecast/daily?"
            f"lat={lat}&lon={lon}&key={config('WEATHERBIT_API_KEY')}&hours=72"
        )
        response = requests.get(url)

        if response.status_code == 429:
            print(f"[{datetime.now()}] Rate limit hit for {postal_code}/{iso_country_code}. Skipping.")
            return None

        if response.status_code != 200:
            print(f"[{datetime.now()}] Request failed: {response.status_code} - {response.text}. Skipping.")
            return None

        return response.json()

    except (requests.RequestException, requests.JSONDecodeError) as e:
        print(f"[{datetime.now()}] Weatherbit request error for {postal_code}/{iso_country_code}: {e}. Skipping.")
        return None


# def extract_from_weatherbit_api(postal_code, iso_country_code):
#     try:
#         lat, lon = get_lat_lon_from_postal_code_iso_country_code(postal_code, iso_country_code)
#         url = f"https://api.weatherbit.io/v2.0/forecast/daily?lat={lat}&lon={lon}&key={config('WEATHERBIT_API_KEY')}&hours=72"
#         response = requests.get(url)
#         return response.json()
#     except (requests.RequestException, requests.JSONDecodeError) as e:
#         print(f"Attempt failed: {e}")
#         # time.sleep(3600)  # wait a bit before retry
#     return None


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
    # lat, lon = coordinates.split(",")
    # lat = float(lat)
    # lon = float(lon)
    # lat, lon = get_wa_lat_lon_from_place_name(place_name)
    # url = f"http://api.weatherapi.com/v1/forecast.json?key={config('WEATHERAPI_API_KEY')}&q={lat},{lon}&days=7&aqi=no&alerts=no"
    url = f"http://api.weatherapi.com/v1/forecast.json?key={config('WEATHERAPI_API_KEY')}&q={lat},{lon}&days=7&aqi=no&alerts=no&pollen=no&tides=no"
    response = requests.get(url)
    return response.json()


def extract_data_from_open_meteo_api(lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,windspeed_10m_max,cloudcover_mean,weathercode&forecast_days=7&timezone=UTC"
    response = requests.get(url)
    return response.json()
