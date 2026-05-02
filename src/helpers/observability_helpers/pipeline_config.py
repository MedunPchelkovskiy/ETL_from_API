PIPELINE_CONFIG = {
    "bronze_openweather": {
        "grain": "day",
        "expected_count": 72,           # 24h * 3 destinations
        "source_check": "azure",
        "max_retries": 10,
    },
    "bronze_tomorrow": {
        "grain": "day",
        "expected_count": 72,
        "source_check": "azure",
        "max_retries": 10,
    },
    "silver": {
        "grain": "day",
        "expected_count": 24,
        "source_check": "postgres",
        "postgres_table":    "silver_summarized_data",
        "postgres_date_col": "forecast_date_utc",
        "max_retries": 5,
    },
    "gold_daily": {
        "grain": "day",
        "expected_count": 24,
        "source_check": "azure+postgres",
        "azure_path_env":    "BASE_DIR_DAILY_SUMM_GOLD",   # year/month/DD.parquet
        "postgres_table":    "gold_daily_summarized_data",
        "postgres_date_col": "forecast_date_utc",
        "max_retries": 5,
    },
    "gold_weekly": {
        "grain": "week",
        "expected_count": 7,
        "source_check": "azure+postgres",
        "azure_path_env":    "BASE_DIR_WEEKLY_SUMM_GOLD",  # year/W{week_number}.parquet
        "postgres_table":    "gold_weekly_summarized_data",
        "postgres_date_col": "week_start",
        "max_retries": 3,
    },
}

PIPELINE_STATUS_MAP = {
    "bronze_openweather": ["pending", "failed"],
    "bronze_tomorrow":    ["pending", "failed"],
    "silver":             ["pending", "failed"],
    "gold_daily":         ["pending", "failed"],
    "gold_weekly":        ["pending", "failed"],
}

PIPELINE_ERROR_MAP = {
    "bronze_openweather": ["api_timeout", "api_rate_limit", "api_error", None],
    "bronze_tomorrow":    ["api_timeout", "api_rate_limit", "api_error", None],
    "silver":             ["transformation_error", "schema_mismatch", "data_type_error", None],
    "gold_daily":         ["insufficient_data", "missing_partitions", None],
    "gold_weekly":        ["insufficient_data", "missing_partitions", None],
}