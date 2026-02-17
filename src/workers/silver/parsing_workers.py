from datetime import timezone, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
from dateutil import parser


def parse_foreca_daily(record: dict) -> Optional[pd.DataFrame]:
    payload = record.get("payload", {})
    if not payload:
        return None

    tz_name = payload.get("tz")  # optional IANA timezone from API

    rows = []
    for i, day in enumerate(payload.get("forecast", [])):
        date_str = day.get("date")

        # --- NEW COLUMNS ---
        if date_str and tz_name:
            # Local datetime with tz
            local_dt = datetime.fromisoformat(date_str).replace(tzinfo=ZoneInfo(tz_name))
            original_ts = local_dt.isoformat()
            # UTC conversion
            utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
            forecast_date_utc = utc_dt.date()
            forecast_hour_utc = None  # daily forecast, no hour
        else:
            original_ts = date_str
            forecast_date_utc = date_str
            forecast_hour_utc = None
        # --- END NEW COLUMNS ---

        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "original_ts": original_ts,
            "forecast_date_utc": forecast_date_utc,
            "forecast_hour_utc": forecast_hour_utc,
            "temp_max": day.get("maxTemp"),
            "temp_min": day.get("minTemp"),
            "temp_avg": None,
            "precipitation": day.get("precipAccum"),
            "rain": None,
            "snow": None,
            "precip_prob": day.get("precipProb"),
            "humidity": None,
            "wind_speed": day.get("maxWindSpeed"),
            "wind_deg": day.get("windDir"),
            "clouds": day.get("cloudiness"),
            "weather_code": None,
            "weather_description": day.get("symbolPhrase"),
            "weather_icon": day.get("symbol"),
            "sunrise": day.get("sunrise"),
            "sunset": day.get("sunset")
        })
    return pd.DataFrame(rows) if rows else None


def parse_accuweather_daily(record: dict) -> Optional[pd.DataFrame]:
    payload = record.get("payload", {})
    if not payload:
        return None
    rows = []
    for i, day in enumerate(payload.get("DailyForecasts", [])):
        temps = day.get("Temperature", {})
        min_temp = temps.get("Minimum", {}).get("Value")
        max_temp = temps.get("Maximum", {}).get("Value")
        day_info = day.get("Day", {})

        original_ts = day.get("Date")
        dt = parser.isoparse(original_ts)
        dt_utc = dt.astimezone(timezone.utc)

        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "original_ts": original_ts,
            "forecast_date_utc": dt_utc.date(),
            "forecast_hour_utc": None,
            "temp_max": max_temp,
            "temp_min": min_temp,
            "temp_avg": None,
            "precipitation": None,
            "rain": None,
            "snow": None,
            "precip_prob": day_info.get("PrecipitationProbability"),
            "humidity": None,
            "wind_speed": None,
            "wind_deg": None,
            "clouds": None,
            "weather_code": None,
            "weather_description": day_info.get("IconPhrase"),
            "weather_icon": day_info.get("Icon"),
            "sunrise": None,
            "sunset": None
        })
    return pd.DataFrame(rows) if rows else None


def parse_tomorrowio_daily(record: dict) -> Optional[pd.DataFrame]:
    payload = record.get("payload", {})
    if not payload:
        return None

    rows = []

    daily_intervals = payload.get("timelines", {}).get("daily", [])

    for interval in daily_intervals:
        values = interval.get("values", {})

        original_ts = interval.get("time")

        dt_utc = None
        if original_ts:
            dt = datetime.fromisoformat(original_ts.replace("Z", "+00:00"))
            dt_utc = dt.astimezone(timezone.utc)  # prevent to save non UTC from APi response

        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "original_ts": original_ts,
            "forecast_date_utc": dt_utc.date(),
            "forecast_hour_utc": None,
            "forecast_date": interval.get("time"),
            "temp_max": values.get("temperatureMax"),
            "temp_min": values.get("temperatureMin"),
            "temp_avg": values.get("temperatureAvg"),
            "precipitation": values.get("rainAccumulationSum"),
            "rain": values.get("rainAccumulationSum"),
            "snow": values.get("snowAccumulationSum"),
            "precip_prob": values.get("precipitationProbabilityAvg"),
            "humidity": values.get("humidityAvg"),
            "wind_speed": values.get("windSpeedAvg"),
            "wind_deg": values.get("windDirectionAvg"),
            "clouds": values.get("cloudCoverAvg"),
            "weather_code": values.get("weatherCodeMax"),
            "weather_description": None,
            "weather_icon": None,
            "sunrise": values.get("sunriseTime"),
            "sunset": values.get("sunsetTime")
        })

    return pd.DataFrame(rows) if rows else None


def parse_meteoblue_basic_day(record: dict) -> Optional[pd.DataFrame]:
    payload = record.get("payload", {})
    if not payload:
        return None
    day_data = payload.get("data_day", {})
    times = day_data.get("time", [])
    temp_max = day_data.get("temperature_max", [])
    temp_min = day_data.get("temperature_min", [])
    temp_mean = day_data.get("temperature_mean", [])
    precip_sum = day_data.get("precipitation_sum", [])

    utc_offset = payload.get("metadata", {}).get("utc_timeoffset", 0)

    rows = []
    for i, date in enumerate(times):
        original_dt = datetime.fromisoformat(date)
        original_ts = original_dt.isoformat()
        forecast_dt_utc = original_dt - timedelta(hours=utc_offset)

        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "original_ts": original_ts,
            "forecast_date_utc": forecast_dt_utc.date(),
            "forecast_hour_utc": None,
            "temp_max": temp_max[i] if i < len(temp_max) else None,
            "temp_min": temp_min[i] if i < len(temp_min) else None,
            "temp_avg": temp_mean[i] if i < len(temp_mean) else None,
            "precipitation": precip_sum[i] if i < len(precip_sum) else None,
            "rain": None,
            "snow": None,
            "precip_prob": None,
            "humidity": None,
            "wind_speed": None,
            "wind_deg": None,
            "clouds": None,
            "weather_code": None,
            "weather_description": None,
            "weather_icon": None,
            "sunrise": None,
            "sunset": None
        })
    return pd.DataFrame(rows) if rows else None


def parse_weatherbit_daily(record: dict) -> Optional[pd.DataFrame]:
    payload = record.get("payload", {})
    if not payload:
        return None

    tz_name = payload.get("timezone", "UTC")
    tz = ZoneInfo(tz_name)

    rows = []
    for i, day in enumerate(payload.get("data", [])):
        weather = day.get("weather", {})

        original_dt = datetime.fromisoformat(day.get("datetime")).replace(tzinfo=tz)
        original_ts = original_dt.isoformat()

        # конверсия в UTC
        forecast_dt_utc = original_dt.astimezone(ZoneInfo("UTC"))

        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "original_ts": original_ts,
            "forecast_date_utc": forecast_dt_utc.date(),
            "forecast_hour_utc": None,
            "temp_max": day.get("max_temp"),
            "temp_min": day.get("min_temp"),
            "temp_avg": None,
            "precipitation": day.get("precip"),
            "rain": None,
            "snow": day.get("snow"),
            "precip_prob": day.get("pop"),
            "humidity": day.get("rh"),
            "wind_speed": day.get("wind_spd"),
            "wind_deg": None,
            "clouds": day.get("clouds"),
            "weather_code": None,
            "weather_description": weather.get("description"),
            "weather_icon": weather.get("icon"),
            "sunrise": None,
            "sunset": None
        })
    return pd.DataFrame(rows) if rows else None


def parse_openweathermap_3h(record: dict) -> Optional[pd.DataFrame]:
    payload = record.get("payload", {})
    if not payload:
        return None

    city_info = payload.get("city", {})
    utc_offset_sec = city_info.get("timezone", 0)
    tz = timezone(timedelta(seconds=utc_offset_sec))
    rows = []
    for entry in payload.get("list", []):
        main = entry.get("main", {})
        weather_arr = entry.get("weather", [])
        weather = weather_arr[0] if weather_arr else {}
        wind = entry.get("wind", {})
        clouds = entry.get("clouds", {})

        original_dt = datetime.fromisoformat(entry.get("dt_txt")).replace(tzinfo=tz)
        original_ts = original_dt.isoformat()

        # конверсия в UTC
        forecast_dt_utc = original_dt.astimezone(ZoneInfo("UTC"))

        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "original_ts": original_ts,
            "forecast_date_utc": forecast_dt_utc.date(),
            "forecast_hour_utc": forecast_dt_utc.hour,
            "temp_max": main.get("temp_max"),
            "temp_min": main.get("temp_min"),
            "temp_avg": main.get("temp"),
            "precipitation": None,
            "rain": None,
            "snow": None,
            "precip_prob": entry.get("pop"),
            "humidity": main.get("humidity"),
            "wind_speed": wind.get("speed"),
            "wind_deg": wind.get("deg"),
            "clouds": clouds.get("all"),
            "weather_code": weather.get("id"),
            "weather_description": weather.get("description"),
            "weather_icon": weather.get("icon"),
            "sunrise": None,
            "sunset": None
        })
    return pd.DataFrame(rows) if rows else None


def parse_weatherapi_daily(record: dict) -> Optional[pd.DataFrame]:
    payload = record.get("payload", {})
    if not payload:
        return None

    tz_name = payload.get("location", {}).get("tz_id", "UTC")
    tz = ZoneInfo(tz_name)

    rows = []
    for day in payload.get("forecast", {}).get("forecastday", []):
        day_info = day.get("day", {})
        condition = day_info.get("condition", {})
        astro_info = day.get("astro", {})

        original_dt = datetime.fromisoformat(day.get('date')).replace(tzinfo=tz)
        forecast_dt_utc = original_dt.astimezone(ZoneInfo("UTC"))

        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "original_ts": original_dt.isoformat(),
            "forecast_date_utc": forecast_dt_utc.date(),
            "forecast_hour_utc": None,
            "temp_max": day_info.get("maxtemp_c"),
            "temp_min": day_info.get("mintemp_c"),
            "temp_avg": day_info.get("avgtemp_c"),
            "precipitation": day_info.get("totalprecip_mm"),
            "rain": None,
            "snow": None,
            "precip_prob": day_info.get("daily_chance_of_rain"),
            "humidity": None,
            "wind_speed": day_info.get("maxwind_kph"),
            "wind_deg": None,
            "clouds": None,
            "weather_code": day_info.get("condition", {}).get("code"),
            "weather_description": condition.get("text"),
            "weather_icon": condition.get("icon"),
            "sunrise": astro_info.get("sunrise"),
            "sunset": astro_info.get("sunset")
        })
    return pd.DataFrame(rows) if rows else None


def parse_open_meteo_daily(record: dict) -> Optional[pd.DataFrame]:
    payload = record.get("payload")
    if not payload:
        return None

    daily = payload.get("daily", {})
    times = daily.get("time", [])
    temp_max = daily.get("temperature_2m_max", [])
    temp_min = daily.get("temperature_2m_min", [])
    precip_sum = daily.get("precipitation_sum", [])
    rain_sum = daily.get("rain_sum", [])
    wind_max = daily.get("windspeed_10m_max", [])
    clouds = daily.get("cloudcover_mean", [])
    weather_code = daily.get("weathercode", [])

    rows = []
    for i, date in enumerate(times):
        dt = datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "original_ts": date,
            "forecast_date_utc": dt.date(),
            "forecast_hour_utc": None,
            "temp_max": temp_max[i] if i < len(temp_max) else None,
            "temp_min": temp_min[i] if i < len(temp_min) else None,
            "temp_avg": None,
            "precipitation": precip_sum[i] if i < len(precip_sum) else None,
            "rain": rain_sum[i] if i < len(rain_sum) else None,
            "snow": None,
            "precip_prob": None,
            "humidity": None,
            "wind_speed": wind_max[i] if i < len(wind_max) else None,
            "wind_deg": None,
            "clouds": clouds[i] if i < len(clouds) else None,
            "weather_code": weather_code[i] if i < len(weather_code) else None,
            "weather_description": None,
            "weather_icon": None,
            "sunrise": None,
            "sunset": None
        })

    return pd.DataFrame(rows) if rows else None
