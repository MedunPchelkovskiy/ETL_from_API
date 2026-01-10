import io
import json

def upload_json(fs_client, base_dir, folder_name, file_name, data):
    """
    Upload a JSON object to a Data Lake folder.
    Creates the directory if it does not exist.
    """
    directory_client = fs_client.get_directory_client(f"{base_dir}/{folder_name}")
    try:
        directory_client.create_directory()
    except Exception:
        # Directory may already exist
        pass

    json_bytes = json.dumps(data).encode("utf-8")
    json_stream = io.BytesIO(json_bytes)

    file_client = directory_client.create_file(file_name)
    file_client.append_data(data=json_stream, offset=0, length=len(json_bytes))
    file_client.flush_data(len(json_bytes))

    print(f"✅ Uploaded {file_name} to folder {folder_name}")
