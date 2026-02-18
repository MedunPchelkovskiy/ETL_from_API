from io import BytesIO

import pandas as pd
from decouple import config


def fetch_silver_parquet_blob(year, month, day, hour, fs_client):
    dir_path = f"{config('BASE_DIR_SILVER')}/{year}/{month}/{day}"
    file_client = fs_client.get_directory_client(dir_path).get_file_client(f"{hour}.parquet")

    try:
        downloaded_bytes = file_client.download_file().readall()
    except FileNotFoundError:
        raise FileNotFoundError(f"Parquet file not found: {year}-{month}-{day}-{hour}")

    df = pd.read_parquet(BytesIO(downloaded_bytes))

    # sanitize UUIDs
    import uuid
    for col in df.columns:
        if df[col].dtype == object and not df[col].empty and isinstance(df[col].iloc[0], uuid.UUID):
            df[col] = df[col].astype(str)

    return df


def fetch_silver_data_postgres(date, hour_int, engine, chunksize=100_000):
    """
    Fetch silver data from Postgres in chunks, sanitize UUIDs.

    Raises ValueError if no data.
    """
    query = """
        SELECT *
        FROM silver_weather_forecast_data
        WHERE ingest_date = %(date)s
          AND ingest_hour >= %(hour)s
        ORDER BY ingest_date DESC, ingest_hour DESC;
    """
    chunks = []
    for chunk in pd.read_sql(query, engine, params={"date": date, "hour": hour_int}, chunksize=chunksize):
        # sanitize UUIDs
        for col in chunk.columns:
            if chunk[col].dtype == object and not chunk[col].empty and isinstance(chunk[col].iloc[0], uuid.UUID):
                chunk[col] = chunk[col].astype(str)
        chunks.append(chunk)

    if not chunks:
        raise ValueError(f"No silver data found for {date}-{hour_int}")

    return pd.concat(chunks, ignore_index=True)
