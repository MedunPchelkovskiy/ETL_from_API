# metrics.py
from prometheus_client import Counter, Histogram, Gauge

# # Bronze layer row count metric
# bronze_rows_extracted = Gauge(
#     "bronze_rows_extracted",
#     "Number of bronze rows extracted per run",
#     ["source", "date", "hour", "flow_run_id", "task_run_id"]
# )
    
FLOW_RUNS = Counter(
    "etl_flow_runs_total",
    "Total flow runs",
    ["flow_name", "status"]
)

FLOW_DURATION = Histogram(
    "etl_flow_duration_seconds",
    "Flow execution time",
    ["flow_name"]
)

TASK_DURATION = Histogram(
    "etl_task_duration_seconds",
    "Task execution time",
    ["flow_name", "task_name"]
)

ROWS_PROCESSED = Counter(
    "etl_rows_processed_total",
    "Rows processed",
    ["flow_name", "task_name"]
)

PIPELINE_RUNNING = Gauge(
    "etl_pipeline_running",
    "ETL pipeline running status",
    ["flow_name"]
)
