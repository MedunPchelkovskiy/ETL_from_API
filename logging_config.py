import logging
import logging.config
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        }
    },
    "handlers": {
        "file": {
            "class": "logging.FileHandler",
            "formatter": "standard",
            "filename": str(LOG_DIR / "etl.log"),
        },
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["file", "console"],
    },
}


def setup_logging():
    logging.config.dictConfig(LOGGING_CONFIG)

    # Also ensure Prefect logs go to file
    file_handler = logging.FileHandler(str(LOG_DIR / "etl.log"))
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )

    prefect_logger = logging.getLogger("prefect")
    prefect_logger.addHandler(file_handler)
    prefect_logger.setLevel(logging.INFO)
