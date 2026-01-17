from prefect import get_run_logger
from prefect.exceptions import Abort
from requests import RequestException

def call_api_with_logging(api_func, *args, name=None, **kwargs):
    logger = get_run_logger()
    display_name = name or "unknown location"
    api_name = api_func.__name__

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
            raise Abort(f"Empty response for {display_name}")

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
        raise Abort(f"Non-retryable error for {display_name}")

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
