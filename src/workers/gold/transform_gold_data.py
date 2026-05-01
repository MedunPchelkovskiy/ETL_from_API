import pandas as pd
import pendulum


def get_daily_summ_data_worker(gold_results: list[tuple[pendulum.DateTime, pd.DataFrame]]) -> list[
    tuple[pendulum.DateTime, pd.DataFrame]]:
    gold_summ_results = []

    for ts, df in gold_results:
        if df.empty:
            continue
        daily_summ_df = df.groupby(["place_name"]).agg(
            temp_max=('temp_max', 'max'),
            temp_min=('temp_min', 'min'),
            temp_avg=('temp_avg', 'mean'),
            wind_speed_max=('wind_speed_max', 'max'),
            wind_speed_min=('wind_speed_min', 'min'),
            wind_speed_avg=('wind_speed_avg', 'mean'),
            rain_max=("rain_max", "max"),
            rain_min=("rain_min", "min"),
            rain_avg=("rain_avg", "mean"),
            snow_max=("snow_max", "max"),
            snow_min=("snow_min", "min"),
            snow_avg=("snow_avg", "mean"),
            cloud_cover_max=("cloud_cover_max", "max"),
            cloud_cover_min=("cloud_cover_min", "min"),
            cloud_cover_avg=("cloud_cover_avg", "mean"),
            humidity_max=("humidity_max", "max"),
            humidity_min=("humidity_min", "min"),
            humidity_avg=("humidity_avg", "mean"),
            ingest_date=('ingest_date', 'max'),
            ingest_hour=('ingest_hour', 'max'),
            forecast_date_utc=('forecast_date_utc', 'first'),
        ).reset_index().round(2)
        daily_summ_df["generated_at"] = pendulum.now("UTC")

        gold_summ_results.append((ts, daily_summ_df))

    return gold_summ_results


def get_weekly_summ_data_worker(week_start, days: list[tuple[pendulum.DateTime, pd.DataFrame]]) -> pd.DataFrame:
    not_empty_df = [df for ts, df in days if not df.empty]

    if len(not_empty_df) < 4:
        raise ValueError(f"Not enough data for week {week_start}, only {len(not_empty_df)} days")

    combined_df = pd.concat(not_empty_df, ignore_index=True)

    weekly_summ_df = combined_df.groupby(["place_name"]).agg(
        temp_max=('temp_max', 'max'),
        temp_min=('temp_min', 'min'),
        temp_avg=('temp_avg', 'mean'),
        wind_speed_max=('wind_speed_max', 'max'),
        wind_speed_min=('wind_speed_min', 'min'),
        wind_speed_avg=('wind_speed_avg', 'mean'),
        rain_max=("rain_max", "max"),
        rain_min=("rain_min", "min"),
        rain_avg=("rain_avg", "mean"),
        snow_max=("snow_max", "max"),
        snow_min=("snow_min", "min"),
        snow_avg=("snow_avg", "mean"),
        cloud_cover_max=("cloud_cover_max", "max"),
        cloud_cover_min=("cloud_cover_min", "min"),
        cloud_cover_avg=("cloud_cover_avg", "mean"),
        humidity_max=("humidity_max", "max"),
        humidity_min=("humidity_min", "min"),
        humidity_avg=("humidity_avg", "mean"),
    ).reset_index().round(2)
    weekly_summ_df["week_number"] = week_start.week_of_year
    weekly_summ_df["year"] = week_start.year
    weekly_summ_df["week_start"] = week_start
    weekly_summ_df["generated_at"] = pendulum.now("UTC")

    return weekly_summ_df
