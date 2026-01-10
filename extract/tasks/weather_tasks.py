from prefect import task
from prefect.context import get_run_context

from extract.workers.extract_data_from_weather_APIs import extract_data_from_foreca_api, extract_data_from_accuweather_api, \
    get_from_meteoblue_api, extract_from_weatherbit_api, extract_data_from_tomorrow_api, \
    extract_data_from_openweathermap_api, extract_data_from_weatherapi_api, extract_data_from_open_meteo_api
from helpers.extraction_helpers.get_accuweather_location_id import get_accuweather_location_id_from_place_name
from helpers.extraction_helpers.get_foreca_location_id import get_foreca_location_id_from_place_name


@task
def get_foreca_data(place_name: str):
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {place_name}"
    api = "foreca_api"
    location_id = get_foreca_location_id_from_place_name(place_name)
    foreca_data = extract_data_from_foreca_api(location_id)
    return {"api": api, "data": foreca_data}


@task
def get_accuweather_data(place_name: str):
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {place_name}"
    api = "accuweather_api"
    location_key = get_accuweather_location_id_from_place_name(place_name)
    accuweather_data = extract_data_from_accuweather_api(location_key)
    return {"api": api, "data": accuweather_data}


@task
def get_meteoblue_data(place_name: str, country: str):
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {place_name}"
    api = "meteoblue_api"
    meteoblue_data = get_from_meteoblue_api(place_name, country)
    return {"api": api, "data": meteoblue_data}


@task
def get_weatherbit_data(postal_code: int, iso_country_code: str):
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {postal_code}"
    api = "weatherbit_api"
    weatherbit_data = extract_from_weatherbit_api(postal_code, iso_country_code)
    return {"api": api, "data": weatherbit_data}


@task
def get_tomorrow_data(place_name: str):
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {place_name}"
    api = "tomorrow_api"
    tomorrow_data = extract_data_from_tomorrow_api(place_name)
    return {"api": api, "data": tomorrow_data}


@task
def get_openweathermap_data(place_name: str, iso_country_code: str):
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {place_name}"
    api = "openweathermap_api"
    openweather_data = extract_data_from_openweathermap_api(place_name, iso_country_code)
    return {"api": api, "data": openweather_data}


@task
def get_weatherapi_data(lat, lon):
    api = "weatherapi_api"
    weatherapi_data = extract_data_from_weatherapi_api(lat, lon)
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {weatherapi_data['location']['name']}"
    return {"api": api, "data": weatherapi_data}


@task
def get_open_meteo_data(lat, lon):
    api = "open_meteo_api"
    open_meteo_data = extract_data_from_open_meteo_api(lat, lon)
    ctx = get_run_context()
    ctx.task_run.name = (
        f"{ctx.task.name} - {lat:.2f},{lon:.2f}"
    )
    return {"api": api, "data": open_meteo_data}