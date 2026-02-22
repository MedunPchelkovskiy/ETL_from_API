import psycopg2
from azure.core.exceptions import ResourceExistsError
from decouple import config


def upload_gold_bytes(fs_client, base_dir, year_folder_name, month_folder_name, day_folder_name, file_name,
                      parquet_bytes):
    """
    Generic uploader: uploads a bytes object to Azure Data Lake.
    Handles directory creation and existence checks.
    Returns dict with upload result.
    """
    directory_path = f"{base_dir}/{year_folder_name}/{month_folder_name}/{day_folder_name}"
    directory_client = fs_client.get_directory_client(directory_path)

    # Ensure directory exists
    try:
        directory_client.create_directory()
    except ResourceExistsError:
        pass

    # File client
    file_client = directory_client.get_file_client(file_name)
    if file_client.exists():
        return {"uploaded": False, "path": f"{directory_path}/{file_name}", "reason": "already_exists"}

    # Upload
    file_client.create_file()
    file_client.append_data(data=parquet_bytes, offset=0, length=len(parquet_bytes))
    file_client.flush_data(len(parquet_bytes))

    return {"uploaded": True, "path": f"{directory_path}/{file_name}", "reason": "created"}


def update_last_processed_timestamp(pipeline_name: str, new_timestamp):
    conn = psycopg2.connect(config("DB_CONN_RAW"))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_metadata (pipeline_name, last_processed_timestamp)
            VALUES (%s, %s)
            ON CONFLICT (pipeline_name)
            DO UPDATE SET
                last_processed_timestamp = EXCLUDED.last_processed_timestamp,
                updated_at = NOW()
            """,
            (pipeline_name, new_timestamp)
        )
    conn.commit()
