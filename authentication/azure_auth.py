from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

def get_datalake_client(tenant_id, client_id, client_secret, account_url, file_system):
    """
    Create and return a Data Lake file system client.
    """
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
    service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
    fs_client = service_client.get_file_system_client(file_system=file_system)
    return fs_client