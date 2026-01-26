# src/workers/silver/parsing_workers.py
from typing import Optional

import pandas as pd

import pandas as pd

def parse_foreca_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Parse Foreca daily forecast JSONs stored in a DataFrame
    with columns: payload, place_name, ingest_date, ingest_hour.
    """

    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        # Get the list of daily forecast entries
        daily_list = payload.get("forecast", [])

        for day in daily_list:
            rows.append({
                "api_name": "foreca_daily",
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
                "date": day.get("date"),

                # Temperatures
                "temp_max": day.get("maxTemp"),
                "temp_min": day.get("minTemp"),
                "feels_like_max": day.get("maxFeelsLikeTemp"),
                "feels_like_min": day.get("minFeelsLikeTemp"),

                # Precipitation
                "precip_accum": day.get("precipAccum"),
                "precip_prob": day.get("precipProb"),

                # Wind
                "max_wind_speed": day.get("maxWindSpeed"),
                "wind_direction": day.get("windDir"),
                "max_wind_gust": day.get("maxWindGust"),

                # Condition symbols/text
                "symbol": day.get("symbol"),
                "symbol_phrase": day.get("symbolPhrase"),

                # Other forecast info
                "cloudiness": day.get("cloudiness"),
                "uv_index": day.get("uvIndex"),
                "pressure": day.get("pressure"),

                # Sunrise, sunset, moon times
                "sunrise": day.get("sunrise"),
                "sunset": day.get("sunset"),
                "moonrise": day.get("moonrise"),
                "moonset": day.get("moonset"),

                # Optional fields (may be null if missing)
                "confidence": day.get("confidence"),
                "min_visibility": day.get("minVisibility"),
                "moon_phase": day.get("moonPhase")
            })

    return pd.DataFrame(rows) if rows else None


def parse_accuweather_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Parse AccuWeather 5-day daily forecast JSONs from payload field into a flat dataframe.
    Input df columns: payload, place_name, ingest_date, ingest_hour.
    """

    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        daily_list = payload.get("DailyForecasts", [])
        for day in daily_list:
            date_iso = day.get("Date")
            temps = day.get("Temperature", {})

            min_temp = temps.get("Minimum", {}).get("Value")
            max_temp = temps.get("Maximum", {}).get("Value")

            day_info = day.get("Day", {})
            night_info = day.get("Night", {})
            sun_info = day.get("Sun", {})

            rows.append({
                "api_name": "accuweather_5day",
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,

                "date": date_iso,
                "temp_min_c": min_temp,
                "temp_max_c": max_temp,

                # Day conditions
                "day_icon": day_info.get("Icon"),
                "day_phrase": day_info.get("IconPhrase"),
                "day_precip_prob": day_info.get("PrecipitationProbability"),

                # Night conditions
                "night_icon": night_info.get("Icon"),
                "night_phrase": night_info.get("IconPhrase"),
                "night_precip_prob": night_info.get("PrecipitationProbability"),

                # Sunrise / Sunset
                "sunrise": sun_info.get("Rise"),
                "sunset": sun_info.get("Set"),

                # Epoch (optional)
                "epoch_date": day.get("EpochDate")
            })

    return pd.DataFrame(rows) if rows else None




def parse_meteoblue_basic_day(api_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Parse meteoblue 'basic-day' forecast JSONs into a flat pandas DataFrame.
    Each row in api_df should contain columns:
    - payload: the JSON dict returned by the API
    - place_name: string
    - ingest_date: date of ingest
    - ingest_hour: hour of ingest
    """

    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        # Extract daily aggregated section
        day_section = payload.get("data_day", {})
        dates = day_section.get("time", [])

        temp_max = day_section.get("temperature_max", [])
        temp_min = day_section.get("temperature_min", [])
        temp_mean = day_section.get("temperature_mean", [])
        precip_sum = day_section.get("precipitation_sum", [])

        # Loop through dates and align fields by index
        for i in range(len(dates)):
            rows.append({
                "api_name": "meteoblue_basic_day",
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
                "date": dates[i],
                "temperature_max": temp_max[i] if i < len(temp_max) else None,
                "temperature_min": temp_min[i] if i < len(temp_min) else None,
                "temperature_mean": temp_mean[i] if i < len(temp_mean) else None,
                "precipitation_sum": precip_sum[i] if i < len(precip_sum) else None,
            })

    return pd.DataFrame(rows) if rows else None



def parse_weatherbit_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Parse Weatherbit daily forecast JSONs stored in a DataFrame
    with columns: payload, place_name, ingest_date, ingest_hour.
    """

    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        # data array of daily forecasts
        daily_list = payload.get("data", [])

        for day in daily_list:
            weather_info = day.get("weather", {})

            rows.append({
                "api_name": "weatherbit_daily",
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,

                "date": day.get("datetime"),
                "temp_max": day.get("max_temp"),
                "temp_min": day.get("min_temp"),
                "precipitation": day.get("precip"),
                "snow": day.get("snow"),
                "precip_prob": day.get("pop"),
                "clouds": day.get("clouds"),

                # Weather text & icon
                "weather_description": weather_info.get("description"),
                "weather_icon": weather_info.get("icon"),

                # Optional additional fields
                "wind_speed": day.get("wind_spd"),
                "humidity": day.get("rh")
            })

    return pd.DataFrame(rows) if rows else None


def parse_tomorrowio_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Parse Tomorrow.io daily forecast JSON responses stored in a DataFrame
    with columns: payload, place_name, ingest_date, ingest_hour.

    Expects payloads from the Tomorrow.io /weather/forecast endpoint using timesteps=1d.
    """

    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        # Safely navigate into the daily timeline
        timelines = payload.get("data", {}).get("timelines", [])
        daily_tl = None
        for tl in timelines:
            # Find the timeline with timestep "1d"
            if tl.get("timestep") == "1d":
                daily_tl = tl
                break

        if not daily_tl:
            continue

        for interval in daily_tl.get("intervals", []):
            start_time = interval.get("startTime")
            values = interval.get("values", {})

            rows.append({
                "api_name": "tomorrowio_daily",
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,

                "date": start_time,

                # Temperatures
                "temp_max": values.get("temperatureMax"),
                "temp_min": values.get("temperatureMin"),

                # Precipitation
                "precip_prob": values.get("precipitationProbability"),
                "precip_intensity": values.get("precipitationIntensity"),

                # Weather conditions (numeric code)
                "weather_code": values.get("weatherCode"),

                # Other commonly useful fields
                "wind_speed": values.get("windSpeed"),
                "humidity": values.get("humidity")
            })

    return pd.DataFrame(rows) if rows else None



def parse_openweathermap_3h(api_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Parse OpenWeatherMap 5-day 3-hour forecast JSONs into a flat pandas DataFrame.

    Input df must contain:
      - payload (the JSON dict)
      - place_name
      - ingest_date
      - ingest_hour
    """

    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        # The 3-hourly list
        forecast_list = payload.get("list", [])

        for entry in forecast_list:
            # basic timing
            dt_txt = entry.get("dt_txt")
            dt_unix = entry.get("dt")

            # main weather fields
            main = entry.get("main", {})
            weather_arr = entry.get("weather", [])
            weather = weather_arr[0] if weather_arr else {}

            wind = entry.get("wind", {})
            clouds = entry.get("clouds", {})

            rows.append({
                "api_name": "openweathermap_3h",
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,

                "forecast_time": dt_txt,
                "forecast_unix": dt_unix,

                # temperature & atmosphere
                "temp": main.get("temp"),
                "temp_min": main.get("temp_min"),
                "temp_max": main.get("temp_max"),
                "humidity": main.get("humidity"),

                # weather description & icon
                "weather_id": weather.get("id"),
                "weather_main": weather.get("main"),
                "weather_description": weather.get("description"),
                "weather_icon": weather.get("icon"),

                # wind
                "wind_speed": wind.get("speed"),
                "wind_deg": wind.get("deg"),

                # clouds & pop
                "cloudiness": clouds.get("all"),
                "pop": entry.get("pop")
            })

    return pd.DataFrame(rows) if rows else None


def parse_weatherapi_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Parse WeatherAPI.com daily forecast JSON responses into a flat DataFrame.

    Input df columns must include:
      - payload: JSON dict
      - place_name: string
      - ingest_date: date
      - ingest_hour: hour
    Returns a Pandas DataFrame or None if no data.
    """

    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        daily_list = payload.get("forecast", {}).get("forecastday", [])

        for day in daily_list:
            date = day.get("date")
            day_info = day.get("day", {})
            astro_info = day.get("astro", {})
            condition = day_info.get("condition", {})

            rows.append({
                "api_name": "weatherapi_daily",
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,

                "date": date,

                # Temperature fields
                "temp_max_c": day_info.get("maxtemp_c"),
                "temp_min_c": day_info.get("mintemp_c"),
                "temp_avg_c": day_info.get("avgtemp_c"),

                # Precipitation / wind
                "total_precip_mm": day_info.get("totalprecip_mm"),
                "daily_chance_of_rain": day_info.get("daily_chance_of_rain"),
                "daily_chance_of_snow": day_info.get("daily_chance_of_snow"),
                "max_wind_kph": day_info.get("maxwind_kph"),

                # Weather condition
                "condition_text": condition.get("text"),
                "condition_icon_url": condition.get("icon"),
                "condition_code": condition.get("code"),

                # Astronomy
                "sunrise": astro_info.get("sunrise"),
                "sunset": astro_info.get("sunset")
            })

    return pd.DataFrame(rows) if rows else None


def parse_open_meteo_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Parse Open‑Meteo daily forecast JSONs into a flat DataFrame.

    Expects `api_df` with columns:
    - payload: the JSON dict returned by Open‑Meteo
    - place_name: identifier for the location
    - ingest_date: ingestion date
    - ingest_hour: ingestion hour
    """

    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        daily = payload.get("daily", {})
        dates = daily.get("time", [])

        temp_max = daily.get("temperature_2m_max", [])
        temp_min = daily.get("temperature_2m_min", [])
        precip_total = daily.get("precipitation_sum", [])
        rain_total = daily.get("rain_sum", [])
        wind_max = daily.get("windspeed_10m_max", [])
        cloud_avg = daily.get("cloudcover_mean", [])
        weather_code = daily.get("weathercode", [])

        # Explode into rows
        for i in range(len(dates)):
            rows.append({
                "api_name": "open_meteo_daily",
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
                "date": dates[i],
                "temperature_max": temp_max[i] if i < len(temp_max) else None,
                "temperature_min": temp_min[i] if i < len(temp_min) else None,
                "precipitation_sum": precip_total[i] if i < len(precip_total) else None,
                "rain_sum": rain_total[i] if i < len(rain_total) else None,
                "wind_speed_max": wind_max[i] if i < len(wind_max) else None,
                "cloudcover_mean": cloud_avg[i] if i < len(cloud_avg) else None,
                "weather_code": weather_code[i] if i < len(weather_code) else None
            })

    return pd.DataFrame(rows) if rows else None
























# def parse_meteoblue(api_df: pd.DataFrame) -> Optional[pd.DataFrame]:
#     if api_df.empty:
#         return None
#
#     rows = []
#
#     for _, row in api_df.iterrows():
#         payload = row["payload"]
#         place_name = row["place_name"]
#         ingest_date = row["ingest_date"]
#         ingest_hour = row["ingest_hour"]
#
#         data_days = payload.get("data_day", [])
#         if not isinstance(data_days, list):
#             continue  # skip invalid payloads
#
#         for day in data_days:
#             if isinstance(day, dict):
#                 rows.append({
#                     "place_name": place_name,
#                     "ingest_date": ingest_date,
#                     "ingest_hour": ingest_hour,
#                     "date": day.get("date"),
#                     "temperature_max": day.get("temperature_max"),
#                     "temperature_min": day.get("temperature_min"),
#                     "precipitation": day.get("precipitation"),
#                 })
#             else:
#                 # day is not a dict, maybe a string or number; log or skip
#                 continue
#
#     if not rows:
#         return None
#
#     df = pd.DataFrame(rows)
#     df["api_name"] = "meteoblue"
#     return df.reset_index(drop=True)
#
#
# def parse_accuweather(api_df: pd.DataFrame) -> pd.DataFrame | None:
#     if api_df.empty:
#         return None
#
#     rows = []
#     for _, row in api_df.iterrows():
#         payload = row["payload"]
#         place_name = row["place_name"]
#         ingest_date = row["ingest_date"]
#         ingest_hour = row["ingest_hour"]
#
#         for forecast in payload.get("DailyForecasts", []):
#             rows.append({
#                 "place_name": place_name,
#                 "ingest_date": ingest_date,
#                 "ingest_hour": ingest_hour,
#                 "date": forecast.get("Date"),
#                 "temperature_max": forecast.get("Temperature", {}).get("Maximum", {}).get("Value"),
#                 "temperature_min": forecast.get("Temperature", {}).get("Minimum", {}).get("Value"),
#                 "precipitation": forecast.get("Day", {}).get("PrecipitationProbability"),
#             })
#
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_open_meteo(api_df: pd.DataFrame) -> pd.DataFrame | None:
#     if api_df.empty:
#         return None
#
#     rows = []
#     for _, row in api_df.iterrows():
#         payload = row["payload"]
#         place_name = row["place_name"]
#         ingest_date = row["ingest_date"]
#         ingest_hour = row["ingest_hour"]
#
#         dates = payload.get("daily", {}).get("time", [])
#         temp_max = payload.get("daily", {}).get("temperature_2m_max", [])
#         temp_min = payload.get("daily", {}).get("temperature_2m_min", [])
#         precipitation = payload.get("daily", {}).get("precipitation_sum", [])
#
#         for i in range(len(dates)):
#             rows.append({
#                 "api_name": "open_meteo",
#                 "place_name": place_name,
#                 "ingest_date": ingest_date,
#                 "ingest_hour": ingest_hour,
#                 "date": dates[i],
#                 "temperature_max": temp_max[i] if i < len(temp_max) else None,
#                 "temperature_min": temp_min[i] if i < len(temp_min) else None,
#                 "precipitation": precipitation[i] if i < len(precipitation) else None,
#             })
#
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_openweathermap(api_df: pd.DataFrame) -> pd.DataFrame | None:
#     if api_df.empty:
#         return None
#
#     rows = []
#     for _, row in api_df.iterrows():
#         payload = row["payload"]
#         place_name = row["place_name"]
#         ingest_date = row["ingest_date"]
#         ingest_hour = row["ingest_hour"]
#
#         for forecast in payload.get("daily", []):
#             rows.append({
#                 "place_name": place_name,
#                 "ingest_date": ingest_date,
#                 "ingest_hour": ingest_hour,
#                 "date": forecast.get("dt"),
#                 "temperature_max": forecast.get("temp", {}).get("max"),
#                 "temperature_min": forecast.get("temp", {}).get("min"),
#                 "precipitation": forecast.get("pop"),
#             })
#
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_tomorrow(api_df: pd.DataFrame) -> pd.DataFrame | None:
#     if api_df.empty:
#         return None
#
#     rows = []
#     for _, row in api_df.iterrows():
#         payload = row["payload"]
#         place_name = row["place_name"]
#         ingest_date = row["ingest_date"]
#         ingest_hour = row["ingest_hour"]
#
#         for forecast in payload.get("data", {}).get("timelines", []):
#             for interval in forecast.get("intervals", []):
#                 values = interval.get("values", {})
#                 rows.append({
#                     "place_name": place_name,
#                     "ingest_date": ingest_date,
#                     "ingest_hour": ingest_hour,
#                     "date": interval.get("startTime"),
#                     "temperature_max": values.get("temperatureMax"),
#                     "temperature_min": values.get("temperatureMin"),
#                     "precipitation": values.get("precipitationProbability"),
#                 })
#
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_weatherapi(api_df: pd.DataFrame) -> pd.DataFrame | None:
#     if api_df.empty:
#         return None
#
#     rows = []
#     for _, row in api_df.iterrows():
#         payload = row["payload"]
#         place_name = row["place_name"]
#         ingest_date = row["ingest_date"]
#         ingest_hour = row["ingest_hour"]
#
#         for day in payload.get("forecast", {}).get("forecastday", []):
#             day_data = day.get("day", {})
#             rows.append({
#                 "place_name": place_name,
#                 "ingest_date": ingest_date,
#                 "ingest_hour": ingest_hour,
#                 "date": day.get("date"),
#                 "temperature_max": day_data.get("maxtemp_c"),
#                 "temperature_min": day_data.get("mintemp_c"),
#                 "precipitation": day_data.get("totalprecip_mm"),
#             })
#
#     return pd.DataFrame(rows) if rows else None
