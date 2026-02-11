from typing import Optional
import pandas as pd


def parse_foreca_daily(record: dict) -> Optional[pd.DataFrame]:
    payload = record.get("payload", {})
    if not payload:
        return None
    rows = []
    for i, day in enumerate(payload.get("forecast", [])):
        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "date": day.get("date"),
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
        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "date": day.get("Date"),
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
    timelines = payload.get("data", {}).get("timelines", [])
    daily_tl = next((tl for tl in timelines if tl.get("timestep") == "1d"), None)
    if not daily_tl:
        return None
    for interval in daily_tl.get("intervals", []):
        values = interval.get("values", {})
        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "date": interval.get("startTime"),
            "temp_max": values.get("temperatureMax"),
            "temp_min": values.get("temperatureMin"),
            "temp_avg": None,
            "precipitation": None,
            "rain": values.get("precipitationIntensity"),
            "snow": None,
            "precip_prob": values.get("precipitationProbability"),
            "humidity": values.get("humidity"),
            "wind_speed": values.get("windSpeed"),
            "wind_deg": None,
            "clouds": None,
            "weather_code": values.get("weatherCode"),
            "weather_description": None,
            "weather_icon": None,
            "sunrise": None,
            "sunset": None
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
    rows = []
    for i, date in enumerate(times):
        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "date": date,
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
    rows = []
    for i, day in enumerate(payload.get("data", [])):
        weather = day.get("weather", {})
        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "date": day.get("datetime"),
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
    rows = []
    for entry in payload.get("list", []):
        main = entry.get("main", {})
        weather_arr = entry.get("weather", [])
        weather = weather_arr[0] if weather_arr else {}
        wind = entry.get("wind", {})
        clouds = entry.get("clouds", {})
        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "date": entry.get("dt_txt"),
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
    rows = []
    for day in payload.get("forecast", {}).get("forecastday", []):
        day_info = day.get("day", {})
        condition = day_info.get("condition", {})
        astro_info = day.get("astro", {})
        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "date": day.get("date"),
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
    payload = record.get("payload", {})
    if not payload:
        return None
    daily = payload.get("daily", {})
    n = len(daily.get("time", []))
    rows = []
    for i in range(n):
        rows.append({
            "api_name": record["source"],
            "place_name": record["place_name"],
            "ingest_date": record["ingest_date"],
            "ingest_hour": record["ingest_hour"],
            "date": daily.get("time", [None]*n)[i],
            "temp_max": daily.get("temperature_2m_max", [None]*n)[i],
            "temp_min": daily.get("temperature_2m_min", [None]*n)[i],
            "temp_avg": None,
            "precipitation": daily.get("precipitation_sum", [None]*n)[i],
            "rain": daily.get("rain_sum", [None]*n)[i],
            "snow": None,
            "precip_prob": None,
            "humidity": None,
            "wind_speed": daily.get("windspeed_10m_max", [None]*n)[i],
            "wind_deg": None,
            "clouds": daily.get("cloudcover_mean", [None]*n)[i],
            "weather_code": daily.get("weathercode", [None]*n)[i],
            "weather_description": None,
            "weather_icon": None,
            "sunrise": None,
            "sunset": None
        })
    return pd.DataFrame(rows) if rows else None
































# from typing import Optional
#
# import pandas as pd
#
#
# # ------------------------------
# # Unified parsers for normalized DF
# # ------------------------------
#
# def parse_foreca_daily(df: pd.DataFrame) -> Optional[pd.DataFrame]:
#     if df.empty:
#         return None
#
#     rows = []
#     for _, row in df.iterrows():
#         # forecast.0.date, forecast.0.maxTemp, etc.
#         forecast_cols = [c for c in row.index if c.startswith("forecast")]
#         days_count = len(forecast_cols) // 7  # adjust depending on how many fields per day
#         for i in range(days_count):
#             rows.append({
#                 "api_name": row["source"],
#                 "place_name": row["place_name"],
#                 "ingest_date": row["ingest_date"],
#                 "ingest_hour": row["ingest_hour"],
#                 "date": row.get(f"forecast.{i}.date"),
#                 "temp_max": row.get(f"forecast.{i}.maxTemp"),
#                 "temp_min": row.get(f"forecast.{i}.minTemp"),
#                 "temp_avg": None,
#                 "precipitation": row.get(f"forecast.{i}.precipAccum"),
#                 "rain": None,
#                 "snow": None,
#                 "precip_prob": row.get(f"forecast.{i}.precipProb"),
#                 "humidity": None,
#                 "wind_speed": row.get(f"forecast.{i}.maxWindSpeed"),
#                 "wind_deg": row.get(f"forecast.{i}.windDir"),
#                 "clouds": row.get(f"forecast.{i}.cloudiness"),
#                 "weather_code": None,
#                 "weather_description": row.get(f"forecast.{i}.symbolPhrase"),
#                 "weather_icon": row.get(f"forecast.{i}.symbol"),
#                 "sunrise": row.get(f"forecast.{i}.sunrise"),
#                 "sunset": row.get(f"forecast.{i}.sunset")
#             })
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_accuweather_daily(df: pd.DataFrame) -> Optional[pd.DataFrame]:
#     if df.empty:
#         return None
#
#     rows = []
#     for _, row in df.iterrows():
#         daily_cols = [c for c in row.index if c.startswith("DailyForecasts")]
#         days_count = len(daily_cols) // 6  # adjust per normalized structure
#         for i in range(days_count):
#             rows.append({
#                 "api_name": row["source"],
#                 "place_name": row["place_name"],
#                 "ingest_date": row["ingest_date"],
#                 "ingest_hour": row["ingest_hour"],
#                 "date": row.get(f"DailyForecasts.{i}.Date"),
#                 "temp_max": row.get(f"DailyForecasts.{i}.Temperature.Maximum.Value"),
#                 "temp_min": row.get(f"DailyForecasts.{i}.Temperature.Minimum.Value"),
#                 "temp_avg": None,
#                 "precipitation": None,
#                 "rain": None,
#                 "snow": None,
#                 "precip_prob": row.get(f"DailyForecasts.{i}.Day.PrecipitationProbability"),
#                 "humidity": None,
#                 "wind_speed": None,
#                 "wind_deg": None,
#                 "clouds": None,
#                 "weather_code": None,
#                 "weather_description": row.get(f"DailyForecasts.{i}.Day.IconPhrase"),
#                 "weather_icon": row.get(f"DailyForecasts.{i}.Day.Icon"),
#                 "sunrise": None,
#                 "sunset": None
#             })
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_tomorrowio_daily(df: pd.DataFrame) -> Optional[pd.DataFrame]:
#     if df.empty:
#         return None
#
#     rows = []
#     intervals_cols = [c for c in df.columns if "intervals" in c]
#     for _, row in df.iterrows():
#         for col in intervals_cols:
#             rows.append({
#                 "api_name": row["source"],
#                 "place_name": row["place_name"],
#                 "ingest_date": row["ingest_date"],
#                 "ingest_hour": row["ingest_hour"],
#                 "date": row.get(col.replace(".values.temperatureMax", ".startTime")),
#                 "temp_max": row.get(col + ".temperatureMax"),
#                 "temp_min": row.get(col + ".temperatureMin"),
#                 "temp_avg": None,
#                 "precipitation": None,
#                 "rain": row.get(col + ".precipitationIntensity"),
#                 "snow": None,
#                 "precip_prob": row.get(col + ".precipitationProbability"),
#                 "humidity": row.get(col + ".humidity"),
#                 "wind_speed": row.get(col + ".windSpeed"),
#                 "wind_deg": None,
#                 "clouds": None,
#                 "weather_code": row.get(col + ".weatherCode"),
#                 "weather_description": None,
#                 "weather_icon": None,
#                 "sunrise": None,
#                 "sunset": None
#             })
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_meteoblue_basic_day(df: pd.DataFrame) -> Optional[pd.DataFrame]:
#     if df.empty:
#         return None
#
#     rows = []
#     for _, row in df.iterrows():
#         dates = row.get("data_day.time", [])
#         temp_max = row.get("data_day.temperature_max", [])
#         temp_min = row.get("data_day.temperature_min", [])
#         temp_mean = row.get("data_day.temperature_mean", [])
#         precip_sum = row.get("data_day.precipitation_sum", [])
#
#         for i in range(len(dates)):
#             rows.append({
#                 "api_name": row["source"],
#                 "place_name": row["place_name"],
#                 "ingest_date": row["ingest_date"],
#                 "ingest_hour": row["ingest_hour"],
#                 "date": dates[i],
#                 "temp_max": temp_max[i] if i < len(temp_max) else None,
#                 "temp_min": temp_min[i] if i < len(temp_min) else None,
#                 "temp_avg": temp_mean[i] if i < len(temp_mean) else None,
#                 "precipitation": precip_sum[i] if i < len(precip_sum) else None,
#                 "rain": None,
#                 "snow": None,
#                 "precip_prob": None,
#                 "humidity": None,
#                 "wind_speed": None,
#                 "wind_deg": None,
#                 "clouds": None,
#                 "weather_code": None,
#                 "weather_description": None,
#                 "weather_icon": None,
#                 "sunrise": None,
#                 "sunset": None
#             })
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_weatherbit_daily(df: pd.DataFrame) -> Optional[pd.DataFrame]:
#     if df.empty:
#         return None
#
#     rows = []
#     for _, row in df.iterrows():
#         data_cols = [c for c in df.columns if c.startswith("data.")]
#         for i, _ in enumerate(data_cols):
#             rows.append({
#                 "api_name": row["source"],
#                 "place_name": row["place_name"],
#                 "ingest_date": row["ingest_date"],
#                 "ingest_hour": row["ingest_hour"],
#                 "date": row.get(f"data.{i}.datetime"),
#                 "temp_max": row.get(f"data.{i}.max_temp"),
#                 "temp_min": row.get(f"data.{i}.min_temp"),
#                 "temp_avg": None,
#                 "precipitation": row.get(f"data.{i}.precip"),
#                 "rain": None,
#                 "snow": row.get(f"data.{i}.snow"),
#                 "precip_prob": row.get(f"data.{i}.pop"),
#                 "humidity": row.get(f"data.{i}.rh"),
#                 "wind_speed": row.get(f"data.{i}.wind_spd"),
#                 "wind_deg": None,
#                 "clouds": row.get(f"data.{i}.clouds"),
#                 "weather_code": None,
#                 "weather_description": row.get(f"data.{i}.weather.description"),
#                 "weather_icon": row.get(f"data.{i}.weather.icon"),
#                 "sunrise": None,
#                 "sunset": None
#             })
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_openweathermap_3h(df: pd.DataFrame) -> Optional[pd.DataFrame]:
#     if df.empty:
#         return None
#
#     rows = []
#     for _, row in df.iterrows():
#         list_cols = [c for c in df.columns if c.startswith("list.")]
#         for i, _ in enumerate(list_cols):
#             rows.append({
#                 "api_name": row["source"],
#                 "place_name": row["place_name"],
#                 "ingest_date": row["ingest_date"],
#                 "ingest_hour": row["ingest_hour"],
#                 "date": row.get(f"list.{i}.dt_txt"),
#                 "temp_max": row.get(f"list.{i}.main.temp_max"),
#                 "temp_min": row.get(f"list.{i}.main.temp_min"),
#                 "temp_avg": row.get(f"list.{i}.main.temp"),
#                 "precipitation": None,
#                 "rain": None,
#                 "snow": None,
#                 "precip_prob": row.get(f"list.{i}.pop"),
#                 "humidity": row.get(f"list.{i}.main.humidity"),
#                 "wind_speed": row.get(f"list.{i}.wind.speed"),
#                 "wind_deg": row.get(f"list.{i}.wind.deg"),
#                 "clouds": row.get(f"list.{i}.clouds.all"),
#                 "weather_code": row.get(f"list.{i}.weather.0.id"),
#                 "weather_description": row.get(f"list.{i}.weather.0.description"),
#                 "weather_icon": row.get(f"list.{i}.weather.0.icon"),
#                 "sunrise": None,
#                 "sunset": None
#             })
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_weatherapi_daily(df: pd.DataFrame) -> Optional[pd.DataFrame]:
#     if df.empty:
#         return None
#
#     rows = []
#     for _, row in df.iterrows():
#         forecast_cols = [c for c in df.columns if "forecast.forecastday" in c]
#         for i, _ in enumerate(forecast_cols):
#             rows.append({
#                 "api_name": row["source"],
#                 "place_name": row["place_name"],
#                 "ingest_date": row["ingest_date"],
#                 "ingest_hour": row["ingest_hour"],
#                 "date": row.get(f"forecast.forecastday.{i}.date"),
#                 "temp_max": row.get(f"forecast.forecastday.{i}.day.maxtemp_c"),
#                 "temp_min": row.get(f"forecast.forecastday.{i}.day.mintemp_c"),
#                 "temp_avg": row.get(f"forecast.forecastday.{i}.day.avgtemp_c"),
#                 "precipitation": row.get(f"forecast.forecastday.{i}.day.totalprecip_mm"),
#                 "rain": None,
#                 "snow": None,
#                 "precip_prob": row.get(f"forecast.forecastday.{i}.day.daily_chance_of_rain"),
#                 "humidity": None,
#                 "wind_speed": row.get(f"forecast.forecastday.{i}.day.maxwind_kph"),
#                 "wind_deg": None,
#                 "clouds": None,
#                 "weather_code": row.get(f"forecast.forecastday.{i}.day.condition.code"),
#                 "weather_description": row.get(f"forecast.forecastday.{i}.day.condition.text"),
#                 "weather_icon": row.get(f"forecast.forecastday.{i}.day.condition.icon"),
#                 "sunrise": row.get(f"forecast.forecastday.{i}.astro.sunrise"),
#                 "sunset": row.get(f"forecast.forecastday.{i}.astro.sunset")
#             })
#     return pd.DataFrame(rows) if rows else None
#
#
# def parse_open_meteo_daily(df: pd.DataFrame) -> Optional[pd.DataFrame]:
#     if df.empty:
#         return None
#
#     rows = []
#     for _, row in df.iterrows():
#         daily_cols = [c for c in df.columns if c.startswith("daily.")]
#         if not daily_cols:
#             continue
#         n = len(row.get("daily.time", []))
#         for i in range(n):
#             rows.append({
#                 "api_name": row["source"],
#                 "place_name": row["place_name"],
#                 "ingest_date": row["ingest_date"],
#                 "ingest_hour": row["ingest_hour"],
#                 "date": row["daily.time"][i],
#                 "temp_max": row.get("daily.temperature_2m_max", [None] * n)[i],
#                 "temp_min": row.get("daily.temperature_2m_min", [None] * n)[i],
#                 "temp_avg": None,
#                 "precipitation": row.get("daily.precipitation_sum", [None] * n)[i],
#                 "rain": row.get("daily.rain_sum", [None] * n)[i],
#                 "snow": None,
#                 "precip_prob": None,
#                 "humidity": None,
#                 "wind_speed": row.get("daily.windspeed_10m_max", [None] * n)[i],
#                 "wind_deg": None,
#                 "clouds": row.get("daily.cloudcover_mean", [None] * n)[i],
#                 "weather_code": row.get("daily.weathercode", [None] * n)[i],
#                 "weather_description": None,
#                 "weather_icon": None,
#                 "sunrise": None,
#                 "sunset": None
#             })
#     return pd.DataFrame(rows) if rows else None
#
#
# # Optional: export dict of all parsers
# UNIFIED_PARSERS = {
#     "foreca": parse_foreca_daily,
#     "accuweather": parse_accuweather_daily,
#     "tomorrowio": parse_tomorrowio_daily,
#     "meteoblue": parse_meteoblue_basic_day,
#     "weatherbit": parse_weatherbit_daily,
#     "openweathermap": parse_openweathermap_3h,
#     "weatherapi": parse_weatherapi_daily,
#     "open_meteo": parse_open_meteo_daily
# }
#
# # import pandas as pd
# #
# #
# # def parse_foreca_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
# #     if api_df.empty:
# #         return None
# #
# #     rows = []
# #     for _, row in api_df.iterrows():
# #         api_name = row["source"]
# #         payload = row["payload"]
# #         place_name = row["place_name"]
# #         ingest_date = row["ingest_date"]
# #         ingest_hour = row["ingest_hour"]
# #
# #         for day in payload.get("forecast", []):
# #             rows.append({
# #                 "api_name": api_name,
# #                 "place_name": place_name,
# #                 "ingest_date": ingest_date,
# #                 "ingest_hour": ingest_hour,
# #                 "date": day.get("date"),
# #                 "temp_max": day.get("maxTemp"),
# #                 "temp_min": day.get("minTemp"),
# #                 "temp_avg": None,
# #                 "precipitation": day.get("precipAccum"),
# #                 "rain": None,
# #                 "snow": None,
# #                 "precip_prob": day.get("precipProb"),
# #                 "humidity": None,
# #                 "wind_speed": day.get("maxWindSpeed"),
# #                 "wind_deg": day.get("windDir"),
# #                 "clouds": day.get("cloudiness"),
# #                 "weather_code": None,
# #                 "weather_description": day.get("symbolPhrase"),
# #                 "weather_icon": day.get("symbol"),
# #                 "sunrise": day.get("sunrise"),
# #                 "sunset": day.get("sunset")
# #             })
# #
# #     return pd.DataFrame(rows) if rows else None
# #
# #
# # def parse_accuweather_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
# #     if api_df.empty:
# #         return None
# #
# #     rows = []
# #     for _, row in api_df.iterrows():
# #         api_name = row["source"]
# #         payload = row["payload"]
# #         place_name = row["place_name"]
# #         ingest_date = row["ingest_date"]
# #         ingest_hour = row["ingest_hour"]
# #
# #         for day in payload.get("DailyForecasts", []):
# #             temps = day.get("Temperature", {})
# #             min_temp = temps.get("Minimum", {}).get("Value")
# #             max_temp = temps.get("Maximum", {}).get("Value")
# #
# #             day_info = day.get("Day", {})
# #             night_info = day.get("Night", {})
# #
# #             rows.append({
# #                 "api_name": api_name,
# #                 "place_name": place_name,
# #                 "ingest_date": ingest_date,
# #                 "ingest_hour": ingest_hour,
# #                 "date": day.get("Date"),
# #                 "temp_max": max_temp,
# #                 "temp_min": min_temp,
# #                 "temp_avg": None,
# #                 "precipitation": None,
# #                 "rain": None,
# #                 "snow": None,
# #                 "precip_prob": day_info.get("PrecipitationProbability"),
# #                 "humidity": None,
# #                 "wind_speed": None,
# #                 "wind_deg": None,
# #                 "clouds": None,
# #                 "weather_code": None,
# #                 "weather_description": day_info.get("IconPhrase"),
# #                 "weather_icon": day_info.get("Icon"),
# #                 "sunrise": None,
# #                 "sunset": None
# #             })
# #
# #     return pd.DataFrame(rows) if rows else None
# #
# #
# # def parse_meteoblue_basic_day(api_df: pd.DataFrame) -> pd.DataFrame | None:
# #     if api_df.empty:
# #         return None
# #
# #     rows = []
# #     for _, row in api_df.iterrows():
# #         api_name = row["source"]
# #         payload = row["payload"]
# #         place_name = row["place_name"]
# #         ingest_date = row["ingest_date"]
# #         ingest_hour = row["ingest_hour"]
# #
# #         day_section = payload.get("data_day", {})
# #         dates = day_section.get("time", [])
# #         temp_max = day_section.get("temperature_max", [])
# #         temp_min = day_section.get("temperature_min", [])
# #         temp_mean = day_section.get("temperature_mean", [])
# #         precip_sum = day_section.get("precipitation_sum", [])
# #
# #         for i in range(len(dates)):
# #             rows.append({
# #                 "api_name": api_name,
# #                 "place_name": place_name,
# #                 "ingest_date": ingest_date,
# #                 "ingest_hour": ingest_hour,
# #                 "date": dates[i],
# #                 "temp_max": temp_max[i] if i < len(temp_max) else None,
# #                 "temp_min": temp_min[i] if i < len(temp_min) else None,
# #                 "temp_avg": temp_mean[i] if i < len(temp_mean) else None,
# #                 "precipitation": precip_sum[i] if i < len(precip_sum) else None,
# #                 "rain": None,
# #                 "snow": None,
# #                 "precip_prob": None,
# #                 "humidity": None,
# #                 "wind_speed": None,
# #                 "wind_deg": None,
# #                 "clouds": None,
# #                 "weather_code": None,
# #                 "weather_description": None,
# #                 "weather_icon": None,
# #                 "sunrise": None,
# #                 "sunset": None
# #             })
# #
# #     return pd.DataFrame(rows) if rows else None
# #
# #
# # def parse_weatherbit_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
# #     if api_df.empty:
# #         return None
# #
# #     rows = []
# #     for _, row in api_df.iterrows():
# #         api_name = row["source"]
# #         payload = row["payload"]
# #         place_name = row["place_name"]
# #         ingest_date = row["ingest_date"]
# #         ingest_hour = row["ingest_hour"]
# #
# #         for day in payload.get("data", []):
# #             weather_info = day.get("weather", {})
# #             rows.append({
# #                 "api_name": api_name,
# #                 "place_name": place_name,
# #                 "ingest_date": ingest_date,
# #                 "ingest_hour": ingest_hour,
# #                 "date": day.get("datetime"),
# #                 "temp_max": day.get("max_temp"),
# #                 "temp_min": day.get("min_temp"),
# #                 "temp_avg": None,
# #                 "precipitation": day.get("precip"),
# #                 "rain": None,
# #                 "snow": day.get("snow"),
# #                 "precip_prob": day.get("pop"),
# #                 "humidity": day.get("rh"),
# #                 "wind_speed": day.get("wind_spd"),
# #                 "wind_deg": None,
# #                 "clouds": day.get("clouds"),
# #                 "weather_code": None,
# #                 "weather_description": weather_info.get("description"),
# #                 "weather_icon": weather_info.get("icon"),
# #                 "sunrise": None,
# #                 "sunset": None
# #             })
# #     return pd.DataFrame(rows) if rows else None
# #
# #
# # def parse_tomorrowio_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
# #     if api_df.empty:
# #         return None
# #
# #     rows = []
# #     for _, row in api_df.iterrows():
# #         api_name = row["source"]
# #         payload = row["payload"]
# #         place_name = row["place_name"]
# #         ingest_date = row["ingest_date"]
# #         ingest_hour = row["ingest_hour"]
# #
# #         timelines = payload.get("data", {}).get("timelines", [])
# #         daily_tl = next((tl for tl in timelines if tl.get("timestep") == "1d"), None)
# #         if not daily_tl:
# #             continue
# #
# #         for interval in daily_tl.get("intervals", []):
# #             values = interval.get("values", {})
# #             rows.append({
# #                 "api_name": api_name,
# #                 "place_name": place_name,
# #                 "ingest_date": ingest_date,
# #                 "ingest_hour": ingest_hour,
# #                 "date": interval.get("startTime"),
# #                 "temp_max": values.get("temperatureMax"),
# #                 "temp_min": values.get("temperatureMin"),
# #                 "temp_avg": None,
# #                 "precipitation": None,
# #                 "rain": values.get("precipitationIntensity"),
# #                 "snow": None,
# #                 "precip_prob": values.get("precipitationProbability"),
# #                 "humidity": values.get("humidity"),
# #                 "wind_speed": values.get("windSpeed"),
# #                 "wind_deg": None,
# #                 "clouds": None,
# #                 "weather_code": values.get("weatherCode"),
# #                 "weather_description": None,
# #                 "weather_icon": None,
# #                 "sunrise": None,
# #                 "sunset": None
# #             })
# #     return pd.DataFrame(rows) if rows else None
# #
# #
# # def parse_openweathermap_3h(api_df: pd.DataFrame) -> pd.DataFrame | None:
# #     if api_df.empty:
# #         return None
# #
# #     rows = []
# #     for _, row in api_df.iterrows():
# #         api_name = row["source"]
# #         payload = row["payload"]
# #         place_name = row["place_name"]
# #         ingest_date = row["ingest_date"]
# #         ingest_hour = row["ingest_hour"]
# #
# #         for entry in payload.get("list", []):
# #             main = entry.get("main", {})
# #             weather_arr = entry.get("weather", [])
# #             weather = weather_arr[0] if weather_arr else {}
# #             wind = entry.get("wind", {})
# #             clouds = entry.get("clouds", {})
# #
# #             rows.append({
# #                 "api_name": api_name,
# #                 "place_name": place_name,
# #                 "ingest_date": ingest_date,
# #                 "ingest_hour": ingest_hour,
# #                 "date": entry.get("dt_txt"),
# #                 "temp_max": main.get("temp_max"),
# #                 "temp_min": main.get("temp_min"),
# #                 "temp_avg": main.get("temp"),
# #                 "precipitation": None,
# #                 "rain": None,
# #                 "snow": None,
# #                 "precip_prob": entry.get("pop"),
# #                 "humidity": main.get("humidity"),
# #                 "wind_speed": wind.get("speed"),
# #                 "wind_deg": wind.get("deg"),
# #                 "clouds": clouds.get("all"),
# #                 "weather_code": weather.get("id"),
# #                 "weather_description": weather.get("description"),
# #                 "weather_icon": weather.get("icon"),
# #                 "sunrise": None,
# #                 "sunset": None
# #             })
# #     return pd.DataFrame(rows) if rows else None
# #
# #
# # def parse_weatherapi_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
# #     if api_df.empty:
# #         return None
# #
# #     rows = []
# #     for _, row in api_df.iterrows():
# #         api_name = row["source"]
# #         payload = row["payload"]
# #         place_name = row["place_name"]
# #         ingest_date = row["ingest_date"]
# #         ingest_hour = row["ingest_hour"]
# #
# #         for day in payload.get("forecast", {}).get("forecastday", []):
# #             day_info = day.get("day", {})
# #             condition = day_info.get("condition", {})
# #             astro_info = day.get("astro", {})
# #             rows.append({
# #                 "api_name": api_name,
# #                 "place_name": place_name,
# #                 "ingest_date": ingest_date,
# #                 "ingest_hour": ingest_hour,
# #                 "date": day.get("date"),
# #                 "temp_max": day_info.get("maxtemp_c"),
# #                 "temp_min": day_info.get("mintemp_c"),
# #                 "temp_avg": day_info.get("avgtemp_c"),
# #                 "precipitation": day_info.get("totalprecip_mm"),
# #                 "rain": None,
# #                 "snow": None,
# #                 "precip_prob": day_info.get("daily_chance_of_rain"),
# #                 "humidity": None,
# #                 "wind_speed": day_info.get("maxwind_kph"),
# #                 "wind_deg": None,
# #                 "clouds": None,
# #                 "weather_code": day_info.get("condition", {}).get("code"),
# #                 "weather_description": condition.get("text"),
# #                 "weather_icon": condition.get("icon"),
# #                 "sunrise": astro_info.get("sunrise"),
# #                 "sunset": astro_info.get("sunset")
# #             })
# #     return pd.DataFrame(rows) if rows else None
# #
# #
# # def parse_open_meteo_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
# #     if api_df.empty:
# #         return None
# #
# #     rows = []
# #     for _, row in api_df.iterrows():
# #         api_name = row["source"]
# #         payload = row["payload"]
# #         place_name = row["place_name"]
# #         ingest_date = row["ingest_date"]
# #         ingest_hour = row["ingest_hour"]
# #
# #         daily = payload.get("daily", {})
# #         dates = daily.get("time", [])
# #         temp_max = daily.get("temperature_2m_max", [])
# #         temp_min = daily.get("temperature_2m_min", [])
# #         precip_total = daily.get("precipitation_sum", [])
# #         rain_total = daily.get("rain_sum", [])
# #         wind_max = daily.get("windspeed_10m_max", [])
# #         cloud_avg = daily.get("cloudcover_mean", [])
# #         weather_code = daily.get("weathercode", [])
# #
# #         for i in range(len(dates)):
# #             rows.append({
# #                 "api_name": api_name,
# #                 "place_name": place_name,
# #                 "ingest_date": ingest_date,
# #                 "ingest_hour": ingest_hour,
# #                 "date": dates[i],
# #                 "temp_max": temp_max[i] if i < len(temp_max) else None,
# #                 "temp_min": temp_min[i] if i < len(temp_min) else None,
# #                 "temp_avg": None,
# #                 "precipitation": precip_total[i] if i < len(precip_total) else None,
# #                 "rain": rain_total[i] if i < len(rain_total) else None,
# #                 "snow": None,
# #                 "precip_prob": None,
# #                 "humidity": None,
# #                 "wind_speed": wind_max[i] if i < len(wind_max) else None,
# #                 "wind_deg": None,
# #                 "clouds": cloud_avg[i] if i < len(cloud_avg) else None,
# #                 "weather_code": weather_code[i] if i < len(weather_code) else None,
# #                 "weather_description": None,
# #                 "weather_icon": None,
# #                 "sunrise": None,
# #                 "sunset": None
# #             })
# #     return pd.DataFrame(rows) if rows else None
