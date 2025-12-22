import requests


def get_lat_lon_from_place_name(place_name, country):
    url = f"https://www.meteoblue.com/en/server/search/query3?query={place_name}"
    response = requests.get(url)
    data = response.json()
    results = data.get("results", [])

    if not results:
        raise ValueError("No search results found")
    for result in results:
        if (result.get("name", "").lower() == place_name.lower() and
            result.get("country", "").lower() == country.lower()):
            return result["lat"], result["lon"]

    return None


