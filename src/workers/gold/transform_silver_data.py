from datetime import datetime

import pandas as pd
import pendulum

from src.helpers.logging_helpers.combine_loggers_helper import get_logger


def get_df_data(silver_results: list, generated_at: pendulum.DateTime) -> list[tuple]:
    logger = get_logger()
    gold_results = []

    for ts, df in silver_results:
        logger.info("forecast_date_utc dtype: %s", df["forecast_date_utc"].dtype)
        logger.info(
            "forecast_date_utc sample types: %s",
            df["forecast_date_utc"].head().apply(lambda x: type(x)).tolist()
        )

        target_date = ts.date()

        df = df[df["forecast_date_utc"] == target_date]
        if df.empty:
            continue

        df_data = df.groupby(["place_name", "forecast_date_utc"]).agg(
            temp_max=('temp_max', 'max'),
            temp_min=('temp_min', 'min'),
            temp_avg=('temp_avg', 'mean'),
            wind_speed_avg=('wind_speed', 'mean'),
            wind_speed_max=('wind_speed', 'max'),
            wind_speed_min=('wind_speed', 'min'),
            rain_min=("rain", "min"),
            rain_max=("rain", "max"),
            rain_avg=("rain", "mean"),
            snow_min=("snow", "min"),
            snow_max=("snow", "max"),
            snow_avg=("snow", "mean"),
            cloud_cover_min=("clouds", "min"),
            cloud_cover_max=("clouds", "max"),
            cloud_cover_avg=("clouds", "mean"),
            humidity_min=("humidity", "min"),
            humidity_max=("humidity", "max"),
            humidity_avg=("humidity", "mean"),
            ingest_date=('ingest_date', 'max'),
            ingest_hour=('ingest_hour', 'max'),
        ).reset_index().round(2)

        df_data["generated_at"] = generated_at
        gold_results.append((ts, df_data))

    return gold_results


def get_fdf_data(silver_results, generated_at: pendulum.DateTime) -> list[tuple]:
    logger = get_logger()
    gold_fd_results = []

    for ts, df in silver_results:
        start = ts.date()
        end = ts.date().add(days=5)

        df["forecast_date_utc"] = pd.to_datetime(df["forecast_date_utc"]).dt.date

        df = df[df["forecast_date_utc"].between(start, end)]

        fdf_data = df.groupby(["place_name", "forecast_date_utc"]).agg(
            temp_max=('temp_max', 'max'),
            temp_min=('temp_min', 'min'),
            temp_avg=('temp_avg', 'mean'),
            wind_speed_avg=('wind_speed', 'mean'),
            wind_speed_max=('wind_speed', 'max'),
            wind_speed_min=('wind_speed', 'min'),
            rain_min=("rain", "min"),
            rain_max=("rain", "max"),
            rain_avg=("rain", "mean"),
            snow_min=("snow", "min"),
            snow_max=("snow", "max"),
            snow_avg=("snow", "mean"),
            cloud_cover_min=("clouds", "min"),
            cloud_cover_max=("clouds", "max"),
            cloud_cover_avg=("clouds", "mean"),
            humidity_min=("humidity", "min"),
            humidity_max=("humidity", "max"),
            humidity_avg=("humidity", "mean"),
            ingest_date=("ingest_date", "max"),
            ingest_hour=("ingest_hour", "max"),
        ).reset_index()

        fdf_data["generated_at"] = generated_at
        gold_fd_results.append((ts, fdf_data))

    return gold_fd_results

