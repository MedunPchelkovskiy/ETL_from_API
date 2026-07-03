import time

from requests import RequestException

from pushgateway_utils import push_api_metrics
from src.helpers.logging_helpers.combine_loggers_helper import get_logger


def call_api_with_logging(api_func, *args, name=None, **kwargs):
    logger = get_logger()
    display_name = name or "unknown location"
    api_name = api_func.__name__
    start_time = time.time()

    try:
        result = api_func(*args, **kwargs)

        if not result:
            logger.error(
                "API returned empty response | api=%s | target=%s",
                api_name,
                display_name,
                extra={
                    "api": api_name,
                    "target": display_name,
                    "result": "empty",
                }
            )
            raise RuntimeError(f"Empty response for {display_name}")

    except RequestException as e:
        logger.warning(
            "API request failed, will retry | api=%s | target=%s | error=%s",
            api_name,
            display_name,
            e,
            exc_info=True,
            extra={
                "api": api_name,
                "target": display_name,
                "retryable": True,
            }
        )
        raise  # Prefect retry

    except Exception as e:
        logger.error(
            "Non-retryable API error | api=%s | target=%s | error=%s",
            api_name,
            display_name,
            e,
            exc_info=True,
            extra={
                "api": api_name,
                "target": display_name,
                "retryable": False,
            }
        )
        raise RuntimeError(f"Non-retryable error for {display_name}")


    finally:
        duration = time.time() - start_time
        logger.info(
            "API duration | api=%s | target=%s | duration=%.3fs",
            api_name,
            display_name,
            duration,
        )
        push_api_metrics(api_name=api_name, location=display_name, duration=duration)

    logger.info(
        "API call succeeded | api=%s | target=%s",
        api_name,
        display_name,
        extra={
            "api": api_name,
            "target": display_name,
            "result": "success",
        }
    )

    return result
