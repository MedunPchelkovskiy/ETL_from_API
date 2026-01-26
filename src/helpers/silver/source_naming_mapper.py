"""
Canonical API mapper: decouples raw source names from internal keys used in mappers, logs, and downstream layers.
"""

canonical_api_map = {
    "foreca_api": "foreca",
    "accuweather_api": "accuweather",
    "meteoblue_api": "meteoblue",
    "open_meteo_api": "open_meteo",
    "openweathermap_api": "openweathermap",
    "tomorrow_api": "tomorrow",
    "weatherapi_api": "weatherapi",
}