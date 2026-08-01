import os
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env.cloud")

KEY_VAULT_URL = "https://weather-etl-kv.vault.azure.net/"

SECRETS = {
    "client-secret": os.getenv("CLIENT_SECRET"),

    "db-user": os.getenv("DB_USER"),
    "db-password": os.getenv("DB_PASSWORD"),
    "db-conn-raw": os.getenv("DB_CONN_RAW"),

    "supabase-db-url": os.getenv("SUPABASE_DB_URL"),

    "foreca-api-key": os.getenv("FORECA_API_KEY"),
    "accuweather-api-key": os.getenv("ACCUWEATHER_API_KEY"),
    "meteoblue-api-key": os.getenv("METEOBLUE_API_KEY"),
    "weatherbit-api-key": os.getenv("WEATHERBIT_API_KEY"),
    "tommorow-api-key": os.getenv("TOMMOROW_API_KEY"),
    "openweathermap-api-key": os.getenv("OPENWEATHERMAP_API_KEY"),
    "weatherapi-api-key": os.getenv("WEATHERAPI_API_KEY"),

    "prefect-api-key": os.getenv("PREFECT_API_KEY"),
}

credential = DefaultAzureCredential()

client = SecretClient(
    vault_url=KEY_VAULT_URL,
    credential=credential
)

for name, value in SECRETS.items():

    if value:
        client.set_secret(name, value)
        print(f"Uploaded: {name}")

    else:
        print(f"Skipped empty secret: {name}")

print("Done.")
