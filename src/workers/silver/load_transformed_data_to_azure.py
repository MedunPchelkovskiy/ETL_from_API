import io

import pandas as pd
from decouple import config

from src.clients.datalake_client import fs_client
from src.helpers.silver.silver_azure_uploader import upload_silver_bytes


def load_silver_data_to_azure_worker(df):
    """
    Converts a Pandas DataFrame to Parquet bytes and uploads to Azure using the base uploader.
    """
    year_str = pd.to_datetime(df["ingest_date"]).dt.strftime("%Y").iloc[0]
    month_str = pd.to_datetime(df["ingest_date"]).dt.strftime("%m").iloc[0]
    day_str = pd.to_datetime(df["ingest_date"]).dt.strftime("%d").iloc[0]
    hour_str = df["ingest_hour"].astype(str).str.zfill(2).iloc[0]

    year_folder_name = f"{year_str}"
    month_folder_name = f"{month_str}"
    day_folder_name = f"{day_str}"
    file_name = f"{hour_str}.parquet"
    # Convert to Parquet bytes
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy")
    parquet_bytes = parquet_buffer.getvalue()

    # Call base uploader
    return upload_silver_bytes(fs_client, config("BASE_DIR_SILVER"), year_folder_name, month_folder_name, day_folder_name,
                               file_name, parquet_bytes)
