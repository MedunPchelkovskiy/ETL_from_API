from prefect import task

from load.raw_data.workers.load_raw_data_from_weather_APIs_to_Azure import upload_json
from load.raw_data.workers.load_raw_data_from_weather_APIs_to_local_postgres import load_raw_api_data_to_postgres


@task
def load_raw_api_data_to_azure_blob(fs_client, base_dir, folder_name, file_name, data):
    upload_json(fs_client, base_dir, folder_name, file_name, data)


@task
def load_raw_api_data_to_postgres_local(data, label):
    load_raw_api_data_to_postgres(data, label)
