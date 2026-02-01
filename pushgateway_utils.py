# pushgateway_utils.py

from prometheus_client import CollectorRegistry, Gauge, Counter, Histogram, push_to_gateway
from typing import Optional
from decouple import config

# Optional: configure Pushgateway URL via environment variable
PUSHGATEWAY_URL = config("PUSHGATEWAY_URL", default="localhost:9091")


def push_metrics_to_gateway(flow_name: str, status: str, duration: float, pushgateway_url: str = PUSHGATEWAY_URL):
    """
    Push flow-level metrics to Prometheus Pushgateway.

    Metrics pushed:
      - Flow runs total (success/failed)
      - Flow execution duration
      - Pipeline running status
    """
    registry = CollectorRegistry()

    # Flow run counter (success / failed)
    flow_runs = Counter(
        "etl_flow_runs_total",
        "Total ETL flow runs",
        ["flow_name", "status"],
        registry=registry
    )
    flow_runs.labels(flow_name=flow_name, status=status).inc()

    # Flow execution duration
    flow_duration = Histogram(
        "etl_flow_duration_seconds",
        "ETL flow execution duration in seconds",
        ["flow_name"],
        registry=registry
    )
    flow_duration.labels(flow_name=flow_name).observe(duration)

    # Pipeline running status (0 = finished)
    pipeline_running = Gauge(
        "etl_pipeline_running",
        "ETL pipeline running status (0=finished, 1=running)",
        ["flow_name"],
        registry=registry
    )
    pipeline_running.labels(flow_name=flow_name).set(0)

    # Push metrics to Pushgateway
    push_to_gateway(pushgateway_url, job=flow_name, registry=registry)


def push_task_metrics(
        flow_name: str,
        task_name: str,
        duration: float,
        rows: int = 0,
        pushgateway_url: str = PUSHGATEWAY_URL
):
    """
    Push task-level metrics to Prometheus Pushgateway.

    Metrics pushed:
      - Task execution duration
      - Rows processed
    """
    registry = CollectorRegistry()

    # Task duration
    task_duration = Histogram(
        "etl_task_duration_seconds",
        "ETL task execution duration in seconds",
        ["flow_name", "task_name"],
        registry=registry
    )
    task_duration.labels(flow_name, task_name).observe(duration)

    # Rows processed
    rows_processed = Counter(
        "etl_rows_processed_total",
        "Number of rows processed per task",
        ["flow_name", "task_name"],
        registry=registry
    )
    rows_processed.labels(flow_name, task_name).inc(rows)

    # Push metrics to Pushgateway
    push_to_gateway(pushgateway_url, job=f"{flow_name}_{task_name}", registry=registry)
