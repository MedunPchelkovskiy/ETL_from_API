import requests
from decouple import config


def get_owm_lat_lon_from_place_name_iso_country_code(place_name, iso_country_code):
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={place_name},{iso_country_code}&limit=1&appid={config('OPENWEATHERMAP_API_KEY')}&lang=en"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Request failed with status code: {response.status_code}")
    else:
        data = response.json()
        lat = data[0]["lat"]
        lon = data[0]["lon"]
        return lat, lon
