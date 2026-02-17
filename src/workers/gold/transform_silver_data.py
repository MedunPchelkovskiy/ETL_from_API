from datetime import date, datetime

import pandas as pd

from src.helpers.logging_helpers.combine_loggers_helper import get_logger


def get_df_data(silver_df) -> pd.DataFrame:
    df_data = pd.DataFrame()
    df_data = silver_df.groupby(["place_name", "ingest_date", "ingest_hour"]).agg(
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
    ).reset_index()

    return df_data


def get_fdf_data(df) -> pd.DataFrame:
    logger = get_logger()
    fdf_group_hourly = pd.DataFrame()
    fdf_data = pd.DataFrame()

    fdf_group_hourly = df.groupby(["place_name", "api_name", "ingest_date", "ingest_hour"]).agg(
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
    ).reset_index()

    today = pd.Timestamp.today().date()
    fdf_group_hourly["ingest_date"] = pd.to_datetime(fdf_group_hourly["ingest_date"]).dt.date

    future_df = fdf_group_hourly[
        fdf_group_hourly["ingest_date"] >= today
        ].copy()

    if future_df.empty:
        logger.error("No future forecast data found for today or next 5 days.")
        raise ValueError("Cannot generate forecast: no future data available")

    next_days = (
        future_df["ingest_date"]
        .drop_duplicates()
        .sort_values()
        .head(5)
    )

    fdf_group_hourly = future_df[
        future_df["ingest_date"].isin(next_days)
    ]

    fdf_data = fdf_group_hourly.groupby(["place_name", "ingest_date", "ingest_hour"]).agg(
        temp_min=('temp_min', 'min'),
        temp_max=('temp_max', 'max'),
        temp_avg=('temp_avg', 'mean'),
        wind_speed_min=('wind_speed_min', 'min'),
        wind_speed_max=('wind_speed_max', 'max'),
        wind_speed_avg=('wind_speed_avg', 'mean'),
        rain_min=("rain_min", "min"),
        rain_max=("rain_max", "max"),
        rain_avg=("rain_avg", "mean"),
        snow_min=("snow_min", "min"),
        snow_max=("snow_max", "max"),
        snow_avg=("snow_avg", "mean"),
        cloud_cover_min=("cloud_cover_min", "min"),
        cloud_cover_max=("cloud_cover_max", "max"),
        cloud_cover_avg=("cloud_cover_avg", "mean"),
        humidity_min=("humidity_min", "min"),
        humidity_max=("humidity_max", "max"),
        humidity_avg=("humidity_avg", "mean"),
    ).reset_index()

    # API count
    api_count = fdf_group_hourly.groupby(["place_name", "ingest_date"])["api_name"].nunique().reset_index(
        name="api_count")
    fdf_data = fdf_data.merge(api_count, on=["place_name", "ingest_date"])

    # Spread columns
    fdf_data["temp_spread"] = fdf_data["temp_max"] - fdf_data["temp_min"]
    fdf_data["rain_spread"] = fdf_data["rain_max"] - fdf_data["rain_min"]
    fdf_data["wind_spread"] = fdf_data["wind_speed_max"] - fdf_data["wind_speed_min"]

    # Metadata
    fdf_data["generated_at"] = datetime.now()
    fdf_data["model_version"] = "v1.0"

    # Data quality
    fdf_data["data_quality"] = "good"
    fdf_data.loc[fdf_data["api_count"] < 2, "data_quality"] = "low_api_coverage"
    fdf_data.loc[fdf_data["temp_spread"] > 8, "data_quality"] = "high_temp_uncertainty"

    return fdf_data
