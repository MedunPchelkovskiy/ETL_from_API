from prefect import task
from prefect.context import get_run_context

from src.workers.bronze.extract_data_from_weather_APIs_workers import extract_data_from_foreca_api, \
    extract_data_from_accuweather_api, \
    get_from_meteoblue_api, extract_data_from_tomorrow_api, \
    extract_data_from_openweathermap_api, extract_data_from_weatherapi_api, extract_data_from_open_meteo_api
from src.helpers.bronze.get_accuweather_location_id import get_accuweather_location_id_from_place_name
from src.helpers.bronze.get_foreca_location_id import get_foreca_location_id_from_place_name
from src.helpers.bronze.task_exception_logger import call_api_with_logging

from src.helpers.logging_helpers.combine_loggers_helper import get_logger


@task(retries=3, retry_delay_seconds=20, )           #add caching to prevent expensive API calls: cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1),
def get_foreca_data(place_name: str):
    logger = get_logger()
    api = "foreca_api"
    logger.info(
        "Task tasks from api=%s started for place_name=%s",
        api,
        place_name,
    )
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {place_name}"

    location_id = get_foreca_location_id_from_place_name(place_name)
    foreca_data = call_api_with_logging(extract_data_from_foreca_api, location_id, name=place_name)

    logger.info(
        "Task tasks from api=%s completed for place_name=%s",
        api,
        place_name,
    )
    return {"api": api, "data": foreca_data}


@task(retries=3, retry_delay_seconds=20, )
def get_accuweather_data(place_name: str):
    api = "accuweather_api"
    logger = get_logger()
    logger.info(
        "Task tasks from api=%s started for place_name=%s",
        api,
        place_name,
    )
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {place_name}"

    location_key = get_accuweather_location_id_from_place_name(place_name)
    accuweather_data = call_api_with_logging(extract_data_from_accuweather_api, location_key, name=place_name)
    logger.info(
        "Task tasks from api=%s completed for place_name=%s",
        api,
        place_name,
    )

    return {"api": api, "data": accuweather_data}


@task(retries=3, retry_delay_seconds=20, )
def get_meteoblue_data(place_name: str, country: str):
    api = "meteoblue_api"
    logger = get_logger()
    logger.info(
        "Task tasks from api=%s started for place_name=%s",
        api,
        place_name,
    )
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {place_name}"

    meteoblue_data = call_api_with_logging(get_from_meteoblue_api, place_name, country, name=place_name)
    logger.info(
        "Task tasks from api=%s completed for place_name=%s",
        api,
        place_name,
    )

    return {"api": api, "data": meteoblue_data}


# @task(retries=3, retry_delay_seconds=20, )
# def get_weatherbit_data(postal_code: int, iso_country_code: str):
#     ctx = get_run_context()
#     ctx.task_run.name = f"{ctx.task.name} - {postal_code}"
#     api = "weatherbit_api"
#     weatherbit_data = call_api_with_logging(extract_from_weatherbit_api, postal_code, iso_country_code,
#                                             name=postal_code)
#     return {"api": api, "data": weatherbit_data}


@task(retries=3, retry_delay_seconds=20, )
def get_tomorrow_data(place_name: str):
    api = "tomorrow_api"
    logger = get_logger()
    logger.info(
        "Task tasks from api=%s started for place_name=%s",
        api,
        place_name,
    )
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {place_name}"

    tomorrow_data = call_api_with_logging(extract_data_from_tomorrow_api, place_name, name=place_name)

    logger.info(
        "Task tasks from api=%s completed for place_name=%s",
        api,
        place_name,
    )

    return {"api": api, "data": tomorrow_data}


@task(retries=3, retry_delay_seconds=20, )
def get_openweathermap_data(place_name: str, iso_country_code: str):
    api = "openweathermap_api"
    logger = get_logger()
    logger.info(
        "Task tasks from api=%s started for place_name=%s",
        api,
        place_name,
    )
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {place_name}"

    openweather_data = call_api_with_logging(extract_data_from_openweathermap_api, place_name, iso_country_code,
                                             name=place_name)
    logger.info(
        "Task tasks from api=%s completed for place_name=%s",
        api,
        place_name,
    )
    return {"api": api, "data": openweather_data}


@task(retries=3, retry_delay_seconds=20, )
def get_weatherapi_data(lat, lon):
    api = "weatherapi_api"
    logger = get_logger()
    logger.info(
        "Task tasks from api=%s started for lat=%s | lon=%s",
        api,
        lat,
        lon,
    )

    weatherapi_data = call_api_with_logging(extract_data_from_weatherapi_api, lat, lon, name=f"{lat:.2f},{lon:.2f}")
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {weatherapi_data['location']['name']}"
    logger.info(
        "Task tasks from api=%s completed for lat=%s | lon=%s",
        api,
        lat,
        lon,
    )
    return {"api": api, "data": weatherapi_data}


@task(retries=3, retry_delay_seconds=20, )
def get_open_meteo_data(lat, lon):
    api = "open_meteo_api"
    logger = get_logger()
    logger.info(
        "Task tasks from api=%s started for lat=%s | lon=%s",
        api,
        lat,
        lon,
    )
    open_meteo_data = call_api_with_logging(extract_data_from_open_meteo_api, lat, lon, name=f"{lat:.2f},{lon:.2f}")
    ctx = get_run_context()
    ctx.task_run.name = f"{ctx.task.name} - {lat:.2f},{lon:.2f}"
    logger.info(
        "Task tasks from api=%s completed for lat=%s | lon=%s",
        api,
        lat,
        lon,
    )
    return {"api": api, "data": open_meteo_data}
