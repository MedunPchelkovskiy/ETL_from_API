import pandas as pd
import pendulum

from src.helpers.logging_helpers.combine_loggers_helper import get_logger


def get_daily_summ_data_worker(gold_results: list) -> list[tuple]:
    gold_summ_results = []

    for ts, df in gold_results:
        if df.empty:
            continue
        daily_summ_df = df.groupby(["place_name"]).agg(
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
            forecast_date_utc=('forecast_date_utc', 'first'),
        ).reset_index().round(2)
        daily_summ_df["generated_at"] = pendulum.now("UTC")

        gold_summ_results.append(daily_summ_df)


    return gold_summ_results
