import logging

from prefect import get_run_logger


def get_logger():
    try:
        logger = get_run_logger()
        logger._logger.propagate = True
        return logger
    except RuntimeError:
        return logging.getLogger(__name__)
