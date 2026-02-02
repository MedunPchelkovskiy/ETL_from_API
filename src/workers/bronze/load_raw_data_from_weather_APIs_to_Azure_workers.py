import io
import json

from azure.core.exceptions import ResourceExistsError

from src.helpers.logging_helpers.combine_loggers_helper import get_logger



def upload_json(fs_client, base_dir, folder_name, file_name, data):
    """
    Upload a JSON object to a Data Lake folder.
    Creates the directory if it does not exist.
    Skips upload if the file already exists.
    Returns a dict indicating upload result for downstream tasks.
    """
    logger = get_logger()
    directory_path = f"{base_dir}/{folder_name}"
    directory_client = fs_client.get_directory_client(directory_path)

    # 1️⃣ Ensure directory exists
    try:
        directory_client.create_directory()
        logger.info("Directory %s created", directory_path, extra={"adls_directory_path": directory_path})
    except ResourceExistsError:
        logger.debug("Directory %s already exists", directory_path, extra={"adls_directory_path": directory_path})

    # 2️⃣ Get file client
    file_client = directory_client.get_file_client(file_name)

    # 3️⃣ Check if file already exists
    if file_client.exists():
        logger.info(
            "Upload to Azure skipped: file %s already exists",
            file_name,
            extra={
                "path": f"{directory_path}/{file_name}",
                "reason": "already_exists",
                "uploaded": False
            }
        )
        return {
            "uploaded": False,
            "path": f"{directory_path}/{file_name}",
            "reason": "already_exists"
        }

    # 4️⃣ Upload file
    json_bytes = json.dumps(data).encode("utf-8")
    json_stream = io.BytesIO(json_bytes)

    file_client.create_file()
    file_client.append_data(data=json_stream, offset=0, length=len(json_bytes))
    file_client.flush_data(len(json_bytes))

    logger.info(
        "Upload completed: Azure blob storage, file=%s, folder=%s",
        file_name,
        directory_path,
        extra={
            "storage": "azure_blob",
            "path": f"{directory_path}/{file_name}",
            "result": "created",
            "uploaded": True,
        }
    )

    return {
        "uploaded": True,
        "path": f"{directory_path}/{file_name}",
        "reason": "created"
    }
