from decouple import config

from src.authentication.azure_auth import get_datalake_client

fs_client = get_datalake_client(
    tenant_id=config("TENANT_ID"),
    client_id=config("CLIENT_ID"),
    client_secret=config("CLIENT_SECRET"),
    account_url=config("ACCOUNT_URL"),
    file_system=config("FILE_SYSTEM")
)