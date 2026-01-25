import pandas as pd


def load_silver_data(df: pd.DataFrame, conn):
    if df is None or df.empty:
        return

    df.to_sql(
        "silver_weather_api_data",
        conn,
        if_exists="append",
        index=False
    )
