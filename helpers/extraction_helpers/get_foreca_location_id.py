import requests
from decouple import config

"""Function that return ID based on provided place name"""

def get_foreca_location_id_from_place_name(place_name):
    url = f"https://pfa.foreca.com/api/v1/location/search/{place_name}?lang=es&token={config("FORECA_API_KEY")}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Request failed with status code: {response.status_code}")
    else:
        data = response.json()
        if data.get("locations"):
          return data["locations"][0]["id"]
        return None


"""
example response from url:
{"locations":[{"id":211001995,"name":"Bansko","country":"Bulgaria","timezone":"Europe\/Sofia",
    "language":"es","adminArea":null,"adminArea2":null,"adminArea3":null,"lon":23.5,
    "lat":41.8394},{"id":100733462,"name":"Bansko","country":"Bulgaria","timezone":"Europe\/Sofia",
     "language":"es","adminArea":"Blagoevgrad","adminArea2":"Obshtina Bansko","adminArea3":null,"lon":23.4885,"lat":41.8383}]}
"""
