# config.py
import os
from dotenv import load_dotenv

env = os.getenv("ENVIRONMENT", "local")
if env == "cloud":
    load_dotenv(".env.cloud", override=True)
else:
    load_dotenv(".env")