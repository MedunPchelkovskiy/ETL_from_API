from prometheus_client import start_http_server
import os

def start_metrics_server():
    port = int(os.getenv("METRICS_PORT", 8000))
    start_http_server(port)
