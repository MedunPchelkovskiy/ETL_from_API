import logging
import logging.config
from pathlib import Path

LOG_DIR = Path("src/logs")
LOG_DIR.mkdir(exist_ok=True)

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        },
        "json": {
            "()": "src.helpers.logging_helpers.json_log_formatter.JSONFormatter",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "file": {
            "class": "logging.FileHandler",
            "formatter": "standard",
            "filename": str(LOG_DIR / "etl.log"),
            "level": "INFO",
        },
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": "INFO",
        },
        "loki": {
            "class": "logging_loki.LokiHandler",
            "formatter": "json",
            "url": "http://localhost:3100/loki/api/v1/push",
            "tags": {"app": "etl-weather"},
            "version": "2",
            "level": "INFO",
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["file", "console", "loki"],
    },
}


def setup_logging():
    logging.config.dictConfig(LOGGING_CONFIG)


    prefect_logger = logging.getLogger("prefect")
    prefect_logger.setLevel(logging.INFO)





















# import logging
# from pathlib import Path
# from logging_loki import LokiHandler
#
# LOG_DIR = Path("src/logs")
# LOG_DIR.mkdir(exist_ok=True)
#
# _INITIALIZED = False
#
#
# def setup_logging():
#     global _INITIALIZED
#     if _INITIALIZED:
#         return
#
#     formatter = logging.Formatter(
#         "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
#     )
#
#     file_handler = logging.FileHandler(LOG_DIR / "etl.log")
#     file_handler.setLevel(logging.INFO)
#     file_handler.setFormatter(formatter)
#
#     loki_handler = LokiHandler(
#         url="http://loki:3100/loki/api/v1/push",
#         tags={"app": "my-application"},
#         version="2",
#     )
#     loki_handler.setLevel(logging.INFO)
#     loki_handler.setFormatter(formatter)
#
#     # Root logger (everything)
#     root = logging.getLogger()
#     root.setLevel(logging.INFO)
#     root.addHandler(file_handler)
#     root.addHandler(loki_handler)
#
#     # Prefect logger (explicit)
#     prefect_logger = logging.getLogger("prefect")
#     prefect_logger.setLevel(logging.INFO)
#     prefect_logger.addHandler(file_handler)
#     prefect_logger.addHandler(loki_handler)
#
#     # 🔥 Prevent double logging
#     prefect_logger.propagate = False
#
#     # Optional but HIGHLY recommended
#     logging.getLogger("httpx").setLevel(logging.WARNING)
#
#     _INITIALIZED = True
#
#
