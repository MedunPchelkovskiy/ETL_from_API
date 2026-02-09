import pandas as pd


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
    fdf_data = pd.DataFrame()

    return fdf_data
