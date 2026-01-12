from prefect import get_run_logger
from prefect.exceptions import Abort
from requests.exceptions import RequestException

def call_api_with_logging(api_func, *args, name=None, **kwargs):
    """
    Wrapper for calling API functions with logging and retry handling.

    - api_func: worker function, call API
    - *args, **kwargs: arguments for API function
    """
    logger = get_run_logger()
    display_name = name or "unknown location"

    try:
        result = api_func(*args, **kwargs)
        if not result:  # If response is None, empty list, empty dict.
           logger.error(f"API call returned empty response for {display_name}")
           raise Abort(f"Empty response for {display_name}")
    except RequestException as exc:
        logger.warning(f"Request failed for {display_name}, will retry: {exc}")
        raise  # Prefect will catch retry
    except Exception as exc:
        logger.error(f"Non-retryable error for {display_name}: {exc}")
        raise Abort(f"Non-retryable error: {exc}")

    logger.info(f"API call succeeded for {display_name}")
    return result
