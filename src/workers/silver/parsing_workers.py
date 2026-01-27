import pandas as pd


def parse_foreca_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        api_name = row["source"]
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        for day in payload.get("forecast", []):
            rows.append({
                "api_name": api_name,
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
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


def parse_accuweather_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        api_name = row["source"]
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        for day in payload.get("DailyForecasts", []):
            temps = day.get("Temperature", {})
            min_temp = temps.get("Minimum", {}).get("Value")
            max_temp = temps.get("Maximum", {}).get("Value")

            day_info = day.get("Day", {})
            night_info = day.get("Night", {})

            rows.append({
                "api_name": api_name,
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
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


def parse_meteoblue_basic_day(api_df: pd.DataFrame) -> pd.DataFrame | None:
    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        api_name = row["source"]
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        day_section = payload.get("data_day", {})
        dates = day_section.get("time", [])
        temp_max = day_section.get("temperature_max", [])
        temp_min = day_section.get("temperature_min", [])
        temp_mean = day_section.get("temperature_mean", [])
        precip_sum = day_section.get("precipitation_sum", [])

        for i in range(len(dates)):
            rows.append({
                "api_name": api_name,
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
                "date": dates[i],
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


def parse_weatherbit_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        api_name = row["source"]
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        for day in payload.get("data", []):
            weather_info = day.get("weather", {})
            rows.append({
                "api_name": api_name,
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
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
                "weather_description": weather_info.get("description"),
                "weather_icon": weather_info.get("icon"),
                "sunrise": None,
                "sunset": None
            })
    return pd.DataFrame(rows) if rows else None


def parse_tomorrowio_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        api_name = row["source"]
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        timelines = payload.get("data", {}).get("timelines", [])
        daily_tl = next((tl for tl in timelines if tl.get("timestep") == "1d"), None)
        if not daily_tl:
            continue

        for interval in daily_tl.get("intervals", []):
            values = interval.get("values", {})
            rows.append({
                "api_name": api_name,
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
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


def parse_openweathermap_3h(api_df: pd.DataFrame) -> pd.DataFrame | None:
    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        api_name = row["source"]
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        for entry in payload.get("list", []):
            main = entry.get("main", {})
            weather_arr = entry.get("weather", [])
            weather = weather_arr[0] if weather_arr else {}
            wind = entry.get("wind", {})
            clouds = entry.get("clouds", {})

            rows.append({
                "api_name": api_name,
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
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


def parse_weatherapi_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        api_name = row["source"]
        payload = row["payload"]
        place_name = row["place_name"]
        ingest_date = row["ingest_date"]
        ingest_hour = row["ingest_hour"]

        for day in payload.get("forecast", {}).get("forecastday", []):
            day_info = day.get("day", {})
            condition = day_info.get("condition", {})
            astro_info = day.get("astro", {})
            rows.append({
                "api_name": api_name,
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
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


def parse_open_meteo_daily(api_df: pd.DataFrame) -> pd.DataFrame | None:
    if api_df.empty:
        return None

    rows = []
    for _, row in api_df.iterrows():
        api_name = row["source"]
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

        for i in range(len(dates)):
            rows.append({
                "api_name": api_name,
                "place_name": place_name,
                "ingest_date": ingest_date,
                "ingest_hour": ingest_hour,
                "date": dates[i],
                "temp_max": temp_max[i] if i < len(temp_max) else None,
                "temp_min": temp_min[i] if i < len(temp_min) else None,
                "temp_avg": None,
                "precipitation": precip_total[i] if i < len(precip_total) else None,
                "rain": rain_total[i] if i < len(rain_total) else None,
                "snow": None,
                "precip_prob": None,
                "humidity": None,
                "wind_speed": wind_max[i] if i < len(wind_max) else None,
                "wind_deg": None,
                "clouds": cloud_avg[i] if i < len(cloud_avg) else None,
                "weather_code": weather_code[i] if i < len(weather_code) else None,
                "weather_description": None,
                "weather_icon": None,
                "sunrise": None,
                "sunset": None
            })
    return pd.DataFrame(rows) if rows else None
