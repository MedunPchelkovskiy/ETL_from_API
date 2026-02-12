import pandas as pd

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.silver.parsing_mapper import api_data_parsers


def normalize_and_combine(records: list[dict], keep_payload: bool = False) -> pd.DataFrame:
    """
    Normalize a list of records (from Azure or Postgres) into a flat DataFrame.

    Each record should have:
        - payload: the original JSON
        - source: API name
        - place_name: location name
        - ingest_date: date of ingestion
        - ingest_hour: hour of ingestion

    Args:
        records: list of dicts
        keep_payload: if True, keep the original JSON in a "payload" column

    Returns:
        pd.DataFrame: flattened DataFrame ready for silver processing
    """
    dfs = []

    for r in records:
        # Skip records with empty payload
        payload = r.get("payload") or {}
        if not payload:
            continue

        # Flatten JSON
        df = pd.json_normalize(payload)

        # Add standard columns
        df["source"] = r.get("source")
        df["place_name"] = r.get("place_name")
        df["ingest_date"] = r.get("ingest_date")
        df["ingest_hour"] = r.get("ingest_hour")

        # Optional: preserve raw JSON
        if keep_payload:
            df["payload"] = [payload] * len(df)

        dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def parse_records_from_api(bronze_records):
    logger = get_logger()
    silver_parts = []

    for record in bronze_records:
        api_name = str(record.get("source", "")).strip()  # ensure string
        if not record:
            logger.warning("Empty record for API: %s", api_name)
            continue

        parser = api_data_parsers.get(api_name)

        if parser:
            df_parsed = parser(record)
            if df_parsed is not None and not df_parsed.empty:
                silver_parts.append(df_parsed)
        else:
            logger.warning("No parser found for source: %r", api_name)
    silver_df = pd.concat(silver_parts, ignore_index=True) if silver_parts else pd.DataFrame()

    return silver_df


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
            logger.info(f"From cleaner worker df columns {date_columns} exists in dataframe")

        else:
            logger.info(f"From cleaner worker df columns {date_columns} not exists in dataframe")

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
# from src.helpers.logging_helpers.combine_loggers_helper import get_logger
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
