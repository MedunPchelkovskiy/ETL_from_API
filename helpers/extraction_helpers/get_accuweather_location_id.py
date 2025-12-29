import requests
import logging
from decouple import config

# Configure logging
logging.basicConfig(
    filename='etl_warnings.log',  # log file for ETL warnings/errors
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_accuweather_location_id_from_place_name(place_name):
    try:
        url = f"https://dataservice.accuweather.com/locations/v1/cities/search?q={place_name}"
        headers = {"Authorization": f"Bearer {config('ACCUWEATHER_API_KEY')}"}
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            logging.warning(f"Request for '{place_name}' failed with status code {response.status_code}")
            return None

        data = response.json()
        if not data:
            logging.warning(f"No location found for '{place_name}'")
            return None

        return data[0].get("Key")  # safely get the first location key

    except requests.RequestException as e:
        logging.warning(f"Request error for '{place_name}': {e}")
        return None
    except ValueError as e:
        logging.warning(f"JSON decode error for '{place_name}': {e}")
        return None
    except Exception as e:
        logging.warning(f"Unexpected error for '{place_name}': {e}")
        return None











#
# import requests
# from decouple import config
#
# """Function that return ID based on provided place name"""

# def get_accuweather_location_id_from_place_name(place_name):
#
#     url = f"https://dataservice.accuweather.com/locations/v1/cities/search?q={place_name}"
#     headers = {"Authorization": f"Bearer {config('ACCUWEATHER_API_KEY')}"}
#     response = requests.get(url, headers=headers)
#     data = response.json()
#     if not data:
#         raise ValueError(f"No location found for {place_name}")
#     if response.status_code != 200:
#         raise Exception(f"Request failed with status code: {response.status_code}")
#     else:
#         data = response.json()
#         return data[1]["Key"]
