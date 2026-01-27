import pandas as pd

from src.helpers.logging_helper.combine_loggers_helper import get_logger


def clean_silver_df(df: pd.DataFrame, debug: bool = False) -> pd.DataFrame:
    """
    Full cleaning and validation for silver-level dataframe:
    - Drops fully empty rows
    - Clips numeric values to safe ranges without dropping rows
    - Normalizes string fields
    - Casts numeric and datetime columns to proper types
    - Logs summaries
    """
    logger = get_logger()

    if df is None or df.empty:
        logger.warning("Clean silver skipped: empty dataframe")
        return df

    before_rows = len(df)

    # Drop fully empty rows only
    df = df.dropna(how='all')

    # -------------------------
    # Numeric fields
    # -------------------------
    numeric_columns = [
        "temp_max", "temp_min", "temp_avg",
        "precipitation", "rain", "snow", "precip_prob",
        "humidity", "wind_speed", "wind_deg", "clouds"
    ]

    # Clip values first, then convert to numeric
    clipping_rules = {
        "temp_max": (-80, 70),
        "temp_min": (-80, 70),
        "temp_avg": (-80, 70),
        "humidity": (0, 100),
        "wind_speed": (0, 200),
        "wind_deg": (0, 360),
        "clouds": (0, 100),
        "precip_prob": (0, 100),
        "precipitation": (0, 1000),
        "rain": (0, 1000),
        "snow": (0, 1000)
    }

    for col in numeric_columns:
        if col in df.columns:
            # Clip out-of-range values to None
            if col in clipping_rules:
                min_val, max_val = clipping_rules[col]
                df[col] = df[col].where(df[col].isna() | ((df[col] >= min_val) & (df[col] <= max_val)), None)
            # Ensure numeric type
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # -------------------------
    # Date/time fields
    # -------------------------
    date_columns = ["forecast_date", "forecast_time"]
    for col in date_columns:
        if col in df.columns:
            if col == "forecast_date":
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            else:
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

    time_columns = ["sunrise", "sunset"]
    for col in time_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.time

    # -------------------------
    # String fields
    # -------------------------
    string_columns = ["api_name", "place_name", "weather_main", "weather_description", "weather_icon"]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    # -------------------------
    # Reset index
    # -------------------------
    df = df.reset_index(drop=True)

    after_rows = len(df)

    logger.info(
        "Silver cleaning and validation completed",
        extra={"rows_before": before_rows, "rows_after": after_rows, "removed": before_rows - after_rows}
    )

    if debug:
        logger.debug(
            "Full dataframe description",
            extra={"describe": df.describe(include='all').to_dict()}
        )

    return df






















# import pandas as pd
#
# from src.helpers.logging_helper.combine_loggers_helper import get_logger
#
#
# def clean_silver_df(df: pd.DataFrame, debug: bool = False) -> pd.DataFrame:
#     """
#     Cleans a silver-level dataframe safely for multi-API data:
#     - Drops fully empty rows
#     - Normalizes numeric types
#     - Clips numeric columns to safe ranges without dropping rows with NaNs
#     - Ensures string fields
#     - Logs summaries
#     """
#     logger = get_logger()
#
#     if df is None or df.empty:
#         logger.warning("Clean silver skipped: empty dataframe")
#         return df
#
#     before_rows = len(df)
#
#     # Drop fully empty rows only
#     df = df.dropna(how='all')
#
#     # Ensure numeric columns exist and convert safely
#     numeric_columns = [
#         "temp_max", "temp_min", "temp_avg",
#         "precipitation", "rain", "snow", "precip_prob",
#         "humidity", "wind_speed", "wind_deg", "clouds"
#     ]
#
#     for col in numeric_columns:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#
#     # Clip numeric columns to safe physical ranges without dropping NaNs
#     clipping_rules = {
#         "temp_max": (-80, 70),
#         "temp_min": (-80, 70),
#         "temp_avg": (-80, 70),
#         "humidity": (0, 100),
#         "wind_speed": (0, 200),
#         "wind_deg": (0, 360),
#         "clouds": (0, 100),
#         "precip_prob": (0, 100),
#         "precipitation": (0, 1000),
#         "rain": (0, 1000),
#         "snow": (0, 1000)
#     }
#
#     for col, (min_val, max_val) in clipping_rules.items():
#         if col in df.columns:
#             df[col] = df[col].where(df[col].isna() | ((df[col] >= min_val) & (df[col] <= max_val)), None)
#
#     # Ensure string columns
#     string_columns = ["api_name", "place_name", "weather_main", "weather_description", "weather_icon"]
#     for col in string_columns:
#         if col in df.columns:
#             df[col] = df[col].astype(str).where(df[col].notna(), None)
#
#     # Reset index
#     df = df.reset_index(drop=True)
#
#     after_rows = len(df)
#
#     logger.info(
#         "Silver cleaning completed",
#         extra={"rows_before": before_rows, "rows_after": after_rows, "removed": before_rows - after_rows}
#     )
#
#     if debug:
#         logger.debug(
#             "Full dataframe description",
#             extra={"describe": df.describe(include='all').to_dict()}
#         )
#
#     return df
#
#
# def validate_silver_df(df: pd.DataFrame) -> pd.DataFrame:
#     # Cast numeric fields
#     numeric_cols = [
#         "temp_max", "temp_min", "temp_avg",
#         "precipitation", "rain", "snow", "pop",
#         "wind_speed", "wind_deg", "clouds", "humidity"
#     ]
#     for col in numeric_cols:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")
#
#     # Cast dates
#     if "forecast_date" in df.columns:
#         df["forecast_date"] = pd.to_datetime(df["forecast_date"], errors="coerce").dt.date
#
#     if "forecast_time" in df.columns:
#         df["forecast_time"] = pd.to_datetime(df["forecast_time"], errors="coerce", utc=True)
#
#     # Cast sunrise/sunset
#     time_cols = ["sunrise", "sunset"]
#     for col in time_cols:
#         if col in df.columns:
#             df[col] = pd.to_datetime(df[col], errors="coerce").dt.time
#
#     # Strings
#     string_cols = ["api_name", "place_name", "weather_main", "weather_description", "weather_icon"]
#     for col in string_cols:
#         if col in df.columns:
#             df[col] = df[col].astype(str).where(df[col].notna(), None)
#
#     return df
