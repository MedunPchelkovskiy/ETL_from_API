import logging
from prefect import get_run_logger

def get_logger():
    try:
        # Inside Prefect task
        return get_run_logger()
    except RuntimeError:
        # Outside Prefect, fallback to normal Python logger
        logger = logging.getLogger(__name__)
        logger.propagate = True
        return logger
