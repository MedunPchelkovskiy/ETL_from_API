import time

from src.helpers.observability_helpers.pushgateway_utils import push_metrics_to_gateway


def measure_flow_duration(flow_name: str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            status = "failed"
            try:
                result = func(*args, **kwargs)
                status = "success"
                return result
            except Exception:
                raise
            finally:
                duration = time.time() - start
                push_metrics_to_gateway(
                    flow_name=flow_name,
                    status=status,
                    duration=duration,
                )
        return wrapper
    return decorator


def measure_task_duration(flow_name: str, task_name: str, on_complete):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            status = "failed"
            try:
                result = func(*args, **kwargs)
                status = "success"
                return result
            except Exception:
                raise
            finally:
                duration = time.time() - start
                on_complete(
                    flow_name=flow_name,
                    task_name=task_name,
                    duration=duration,
                )
        return wrapper
    return decorator
