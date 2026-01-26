# import pandas as pd
# from prefect import get_run_logger
# from sqlalchemy.exc import SQLAlchemyError


from sqlalchemy import text
import pandas as pd
from prefect import get_run_logger
from sqlalchemy.exc import SQLAlchemyError

def load_silver_data(df: pd.DataFrame, engine):
    logger = get_run_logger()

    if df is None or df.empty:
        logger.warning("Silver load skipped: empty dataframe")
        return

    # Ensure source column exists
    if "api_name" not in df.columns:
        df["api_name"] = df.get("source_api")

    cols = [
        "api_name", "place_name", "ingest_date", "ingest_hour",
        "forecast_date", "forecast_time",
        "temp_max", "temp_min", "temp_avg",
        "precipitation", "rain", "snow", "pop",
        "wind_speed", "wind_deg",
        "clouds", "humidity",
        "weather_code", "weather_main", "weather_description", "weather_icon",
        "sunrise", "sunset"
    ]

    for col in cols:
        if col not in df.columns:
            df[col] = None

    stmt = """
    INSERT INTO silver_weather_forecast_data (
        api_name, place_name, ingest_date, ingest_hour,
        forecast_date, forecast_time,
        temp_max, temp_min, temp_avg,
        precipitation, rain, snow, pop,
        wind_speed, wind_deg,
        clouds, humidity,
        weather_code, weather_main, weather_description, weather_icon,
        sunrise, sunset
    ) VALUES (
        :api_name, :place_name, :ingest_date, :ingest_hour,
        :forecast_date, :forecast_time,
        :temp_max, :temp_min, :temp_avg,
        :precipitation, :rain, :snow, :pop,
        :wind_speed, :wind_deg,
        :clouds, :humidity,
        :weather_code, :weather_main, :weather_description, :weather_icon,
        :sunrise, :sunset
    )
    ON CONFLICT (api_name, place_name, forecast_date, forecast_time) DO UPDATE
    SET
        temp_max = EXCLUDED.temp_max,
        temp_min = EXCLUDED.temp_min,
        temp_avg = EXCLUDED.temp_avg,
        precipitation = EXCLUDED.precipitation,
        rain = EXCLUDED.rain,
        snow = EXCLUDED.snow,
        pop = EXCLUDED.pop,
        wind_speed = EXCLUDED.wind_speed,
        wind_deg = EXCLUDED.wind_deg,
        clouds = EXCLUDED.clouds,
        humidity = EXCLUDED.humidity,
        weather_code = EXCLUDED.weather_code,
        weather_main = EXCLUDED.weather_main,
        weather_description = EXCLUDED.weather_description,
        weather_icon = EXCLUDED.weather_icon,
        sunrise = EXCLUDED.sunrise,
        sunset = EXCLUDED.sunset,
        ingest_date = EXCLUDED.ingest_date,
        ingest_hour = EXCLUDED.ingest_hour;
    """

    def _sanitize(records):
        return [
            {k: (None if v is pd.NaT or v == "NaT" else v) for k, v in row.items()}
            for row in records
        ]

    try:
        for start in range(0, len(df), 1000):
            batch = df.iloc[start:start + 1000]
            values = _sanitize(batch.to_dict(orient="records"))

            with engine.begin() as connection:
                connection.execute(text(stmt), values)

        logger.info(
            "Silver data loaded successfully",
            extra={"rows": len(df), "table": "silver_weather_forecast_data"}
        )

    except SQLAlchemyError:
        logger.error("Failed to load silver data", exc_info=True)
        raise
