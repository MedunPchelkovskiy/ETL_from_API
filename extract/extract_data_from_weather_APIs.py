import time

import requests
from decouple import config

from helpers.meteoblue_coordinates_finder import get_lat_lon_from_place_name


def extract_data_from_foreca_api(location_id:int, max_retries: int =5):
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


