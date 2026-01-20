from os import WCONTINUED

import requests
from decouple import config
from sqlalchemy_utils.types.password import passlib


def get_wa_lat_lon_from_place_name(place_name):
    url = f"https://api.weatherapi.com/v1/search.json?key={config('WEATHERAPI_API_KEY')}&q={place_name}"
    response = requests.get(url)
    data=response.json()
    if not data:  # No results returned
        return None, None
    lat = data[0]["lat"]
    lon = data[0]["lon"]
    return lat, lon
