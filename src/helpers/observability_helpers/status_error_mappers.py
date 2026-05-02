PIPELINE_STATUS_MAP = {
    "bronze": ["failed"],
    "silver": ["failed"],
    "gold_daily": ["failed"],
    "gold_weekly": ["failed"],
}
PIPELINE_ERROR_MAP = {
    "bronze": [
        "api_timeout",
        "api_rate_limit",
        "api_error",
    ],
    "silver": [
        "transformation_error",
        "schema_mismatch",
        "data_type_error",
    ],
    "gold_daily": [
        "insufficient_data",
        "missing_partitions",
    ],
    "gold_weekly": [
        "insufficient_data",
        "missing_partitions",
    ],
}