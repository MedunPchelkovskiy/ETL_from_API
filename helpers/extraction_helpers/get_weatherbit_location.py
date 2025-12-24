import requests
from decouple import config


def get_lat_lon_from_postal_code_iso_country_code(postal_code, iso_country_code):
    url = f"https://api.weatherbit.io/v2.0/geocode?postal_code={postal_code}&country={iso_country_code}&key={config('WEATHERBIT_API_KEY')}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Request failed with status code: {response.status_code}")
    else:
        data = response.json()
        return data["lat"], data["lon"]

