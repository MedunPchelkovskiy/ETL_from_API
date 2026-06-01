import pendulum

completed_months = pendulum.now("UTC").month - 1

max_missing_count = min(completed_months // 3, 4)

QUARTER_START_MONTH = {3: 'spring', 6: 'summer', 9: 'autumn', 12: 'winter'}

PIPELINE_CONFIG = {
    "bronze_openweather": {
        "grain": "day",
        "expected_count": 72,  # 24h * 3 destinations
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
        "postgres_table": "silver_summarized_data",
        "postgres_date_col": "forecast_date_utc",
        "max_retries": 5,
    },
    "gold_daily": {
        "grain": "day",
        "expected_count": 24,
        "source_check": "azure+postgres",
        "azure_path_env": "BASE_DIR_DAILY_SUMM_GOLD",  # year/month/DD.parquet
        "postgres_table": "gold_daily_summarized_data",
        "postgres_date_col": "forecast_date_utc",
        "max_retries": 5,
    },
    "gold_weekly": {
        "grain": "week",
        "expected_count": 7,
        "source_check": "azure+postgres",
        "azure_path_env": "BASE_DIR_WEEKLY_SUMM_GOLD",  # year/W{week_number}.parquet
        "postgres_table": "gold_weekly_summarized_data",
        "postgres_date_col": "week_start",
        "max_retries": 3,
    },
    "gold_monthly": {
        "grain": "month",
        "max_missing_ratio": 0.15,
        # "expected_count": 12,
        "source_check": "azure+postgres",
        "azure_path_env": "BASE_DIR_MONTHLY_SUMM_GOLD",  # year/MM.parquet
        "postgres_table": "gold_monthly_summarized_data",
        "postgres_date_col": "month_start",
        "max_retries": 3,
    },
    "gold_yearly": {
        "grain": "month",
        "max_missing_ratio":
            max_missing_count / completed_months
            if completed_months > 0
            else 0,
        # "expected_count": 12,
        "expected_count": completed_months,
        "source_check": "azure+postgres",
        "azure_path_env": "BASE_DIR_YEARLY_SUMM_GOLD",  # year.parquet
        "postgres_table": "gold_yearly_summarized_data",
        "postgres_date_col": "year_start",
        "max_retries": 3,
        },
    "gold_yearly_seasonal": {
        "grain": "month",
        "max_missing_ratio": 0.33,
        "expected_count": 3,
        "source_check": "azure+postgres",
        "azure_path_env": "BASE_DIR_YEARLY_SUMM_GOLD",  # Q(1, 2, 3, 4).parquet
        "postgres_table": "gold_yearly_summarized_data",
        "postgres_date_col": "year_start",
        "max_retries": 3,
    },
}

PIPELINE_STATUS_MAP = {
    "bronze_openweather": ["pending", "failed"],
    "bronze_tomorrow": ["pending", "failed"],
    "silver": ["pending", "failed"],
    "gold_daily": ["pending", "failed", "partial"],
    "gold_weekly": ["pending", "failed", "partial"],
    "gold_monthly": ["pending", "failed", "partial"],
    "gold_yearly": ["pending", "failed", "partial"],
    "gold_yearly_Q": ["pending", "failed", "partial"],
}

PIPELINE_ERROR_MAP = {
    "bronze_openweather": ["api_timeout", "api_rate_limit", "api_error", None],
    "bronze_tomorrow": ["api_timeout", "api_rate_limit", "api_error", None],
    "silver": ["transformation_error", "schema_mismatch", "data_type_error", None],
    "gold_daily": ["insufficient_data", "missing_partitions", None],
    "gold_weekly": ["insufficient_data", "missing_partitions", None],
    "gold_monthly": ["insufficient_data", "missing_partitions", None],
    "gold_yearly": ["insufficient_data", "missing_partitions", None],
    "gold_yearly_Q": ["insufficient_data", "missing_partitions", None],
}
