LOCATIONS = [
    {
        "name": "Bansko",
        "lat": 41.77,
        "lon": 23.43,
        "country_code": "BG",
        "country_name": "Bulgaria",
    },
    {
        "name": "Pamporovo",
        "lat": 41.65,
        "lon": 24.69,
        "country_code": "BG",
        "country_name": "Bulgaria",
    },
    {
        "name": "Borovets",
        "lat": 42.27,
        "lon": 23.60,
        "country_code": "BG",
        "country_name": "Bulgaria",
    },
]



def build_api_locations(locations):
    return {
        "accuweather": [
            (loc["name"], loc["name"])  # API expects just city name
            for loc in locations
        ],
        "meteoblue": [
            (loc["name"], (loc["name"], loc["country_name"]))  # needs full country name
            for loc in locations
        ],
        "tomorrow": [
            (loc["name"], loc["name"])  # city name only
            for loc in locations
        ],
        "openweathermap": [
            (loc["name"], (loc["name"], loc["country_code"]))  # needs country code
            for loc in locations
        ],
        "weatherapi": [
            (loc["name"], (loc["lat"], loc["lon"]))  # lat/lon
            for loc in locations
        ],
        "open_meteo": [
            (loc["name"], (loc["lat"], loc["lon"]))  # lat/lon
            for loc in locations
        ],
    }

api_locations = build_api_locations(LOCATIONS)





# api_locations = {
#     "accuweather": [
#         ("Bansko", "Bansko"),
#         ("Pamporovo", "Pamporovo"),
#         ("Borovets", "Borovets"),
#     ],
#     "meteoblue": [
#         ("Bansko", ("Bansko", "Bulgaria")),
#         ("Pamporovo", ("Pamporovo", "Bulgaria")),
#         ("Borovets", ("Borovets", "Bulgaria")),
#     ],
#     "tomorrow": [
#         ("Bansko", "Bansko"),
#         ("Pamporovo", "Pamporovo"),
#         ("Borovets", "Borovets"),
#     ],
#     "openweathermap": [
#         ("Bansko", ("Bansko", "BG")),
#         ("Pamporovo", ("Pamporovo", "BG")),
#         ("Borovets", ("Borovets", "BG")),
#     ],
#     "weatherapi": [
#         ("Bansko", (41.77, 23.43)),
#         ("Pamporovo", (41.65, 24.69)),
#         ("Borovets", (42.27, 23.60)),
#     ],
#     "open_meteo": [
#         ("Bansko", (41.77, 23.43)),
#         ("Pamporovo", (41.65, 24.69)),
#         ("Borovets", (42.27, 23.60)),
#     ],
# }
