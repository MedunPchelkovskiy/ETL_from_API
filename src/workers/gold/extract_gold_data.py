from io import BytesIO

import pandas as pd
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from decouple import config


def get_hourly_blobs_for_today(year, month, day, fs_client) -> pd.DataFrame:
    df = pd.DataFrame()

    day_path = f"{config('BASE_DIR_GOLD')}/{year}/{month:02d}/{day:02d}"
    directory_client = fs_client.get_directory_client(day_path)
    try:
        paths = list(directory_client.get_paths())
    except ResourceNotFoundError as e:
        raise RuntimeError(
            f"Expected partition missing: {day_path}"
        ) from e

    if not paths:
        raise RuntimeError(
            f"Partition exists but contains no files: {day_path}"
        )
    files = [path.name for path in paths if not path.is_directory and path.name.endswith(".parquet")]
    files = sorted(files)

    dfs = []
    for file in files:
        try:                                               # just safety check if file is deleted between listing and fetching
            file_client = fs_client.get_file_client(file)
            downloaded_bytes = file_client.download_file().readall()
        except ResourceNotFoundError:
            raise FileNotFoundError(f"Parquet file not found: {file}")

        curr_df = pd.read_parquet(BytesIO(downloaded_bytes))
        dfs.append(curr_df)

    df = pd.concat(dfs, ignore_index=True)
    return df
