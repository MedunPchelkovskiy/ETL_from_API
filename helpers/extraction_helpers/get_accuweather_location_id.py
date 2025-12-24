import requests
from decouple import config

"""Function that return ID based on provided place name"""

def get_accuweather_location_id_from_place_name(place_name):

    url = f"https://dataservice.accuweather.com/locations/v1/cities/search?q={place_name}"
    headers = {"Authorization": f"Bearer {config('ACCUWEATHER_API_KEY')}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Request failed with status code: {response.status_code}")
    else:
        data = response.json()
        return data[1]["Key"]
