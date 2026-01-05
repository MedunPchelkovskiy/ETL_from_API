from datetime import datetime

import requests
from decouple import config


def get_lat_lon_from_postal_code_iso_country_code(postal_code, iso_country_code):
    """
    Returns (lat, lon) tuple for a postal code + country.
    Returns None if request fails or 429 hit.
    """
    try:
        response = requests.get(f"https://some-geocoding-api?postal_code={postal_code}&country={iso_country_code}")

        if response.status_code == 429:
            print(f"[{datetime.now()}] Rate limit hit for {postal_code}/{iso_country_code}. Skipping location.")
            return None

        if response.status_code != 200:
            print(f"[{datetime.now()}] Request failed: {response.status_code} - {response.text}")
            return None

        data = response.json()
        return data["lat"], data["lon"]

    except (requests.RequestException, requests.JSONDecodeError) as e:
        print(f"[{datetime.now()}] Request error for {postal_code}/{iso_country_code}: {e}. Skipping.")
        return None


# def get_lat_lon_from_postal_code_iso_country_code(postal_code, iso_country_code):
#     url = f"https://api.weatherbit.io/v2.0/geocode?postal_code={postal_code}&country={iso_country_code}&key={config('WEATHERBIT_API_KEY')}"
#     response = requests.get(url)
#     if response.status_code != 200:
#         raise Exception(f"Request failed with status code: {response.status_code}")
#     else:
#         data = response.json()
#         return data["lat"], data["lon"]

