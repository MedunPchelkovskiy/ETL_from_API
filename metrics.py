from prometheus_client import Gauge

# Bronze layer row count metric
bronze_rows_extracted = Gauge(
    "bronze_rows_extracted",
    "Number of bronze rows extracted per run",
    ["source", "date", "hour", "flow_run_id", "task_run_id"]
)
