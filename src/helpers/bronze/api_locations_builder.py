def build_api_locations(locations):
    return {
        "accuweather": [
            (loc["name"], loc["name"])
            for loc in locations
        ],
        "meteoblue": [
            (loc["name"], (loc["name"], "Bulgaria"))
            for loc in locations
        ],
        "tomorrow": [
            (loc["name"], loc["name"])
            for loc in locations
        ],
        "openweathermap": [
            (loc["name"], (loc["name"], loc["country"]))
            for loc in locations
        ],
        "weatherapi": [
            (loc["name"], (loc["lat"], loc["lon"]))
            for loc in locations
        ],
        "open_meteo": [
            (loc["name"], (loc["lat"], loc["lon"]))
            for loc in locations
        ],
    }


