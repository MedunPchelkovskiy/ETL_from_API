from datetime import date, time

import numpy as np
import pandas as pd

from src.helpers.logging_helper.combine_loggers_helper import get_logger


def clean_silver_df(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_logger()

    if df is None or df.empty:
        logger.warning("Clean silver skipped: empty dataframe")
        return df

    before_rows = len(df)

    # Drop duplicates and fully empty rows
    df = df.drop_duplicates().dropna(how="all")

    # Ensure required columns exist and are not null
    required_cols = ["forecast_date", "place_name", "api_name"]
    for col in required_cols:
        if col in df.columns:
            df = df[df[col].notna()]

    # Numeric type normalization
    numeric_columns = [
        "temp_max", "temp_min", "temp_avg",
        "precipitation", "rain", "snow", "pop",
        "wind_speed", "wind_deg",
        "clouds", "humidity"
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Physical sanity checks
    if "temp_max" in df.columns:
        df = df[(df["temp_max"] > -80) & (df["temp_max"] < 70)]
    if "temp_min" in df.columns:
        df = df[(df["temp_min"] > -80) & (df["temp_min"] < 70)]
    if "humidity" in df.columns:
        df = df[(df["humidity"] >= 0) & (df["humidity"] <= 100)]
    if "wind_speed" in df.columns:
        df = df[df["wind_speed"] >= 0]
    if "precipitation" in df.columns:
        df = df[df["precipitation"] >= 0]
    if "rain" in df.columns:
        df = df[df["rain"] >= 0]
    if "snow" in df.columns:
        df = df[df["snow"] >= 0]

    # Datetime normalization with default fallback
    df = df.astype(object)

    # Replace ALL pandas missing values
    df = df.where(pd.notna(df), None)

    # Ensure string fields
    string_columns = [
        "api_name", "place_name", "weather_main",
        "weather_description", "weather_icon"
    ]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    # Reset index
    df = df.reset_index(drop=True)
    after_rows = len(df)

    logger.info(
        "Silver cleaning completed",
        extra={
            "rows_before": before_rows,
            "rows_after": after_rows,
            "removed": before_rows - after_rows,
        }
    )

    return df















# def clean_silver_df(df: pd.DataFrame) -> pd.DataFrame:
#     logger = get_logger()
#
#     if df is None or df.empty:
#         logger.warning("Clean silver skipped: empty dataframe")
#         return df
#
#     before_rows = len(df)
#
#     df = df.drop_duplicates()  # 1. Drop duplicates
#     df = df.dropna(how="all")  # 2. Remove fully empty rows
#     required_cols = ["event_dt", "place_name", "api_name"]  # 3. Basic null handling
#
#     for col in required_cols:
#         if col in df.columns:
#             df = df[df[col].notna()]
#
#     type_map = {  # 4. Type normalization
#         "temp": "float",
#         "humidity": "float",
#         "wind_speed": "float",
#         "pressure": "float",
#         "uv_index": "float",
#     }
#
#     for col, dtype in type_map.items():
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")
#
#     if "temp" in df.columns:  # 5. Physical sanity rules
#         df = df[(df["temp"] > -80) & (df["temp"] < 70)]
#
#     if "humidity" in df.columns:
#         df = df[(df["humidity"] >= 0) & (df["humidity"] <= 100)]
#
#     if "wind_speed" in df.columns:
#         df = df[df["wind_speed"] >= 0]
#
#     if "event_dt" in df.columns:  # 6. Datetime normalization
#         df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
#         df = df[df["event_dt"].notna()]
#
#     df = df.reset_index(drop=True)  # 7. Reset index
#
#     after_rows = len(df)
#
#     logger.info(
#         "Silver cleaning completed",
#         extra={
#             "rows_before": before_rows,
#             "rows_after": after_rows,
#             "removed": before_rows - after_rows,
#         }
#     )
#
#     return df


def validate_silver_df(df: pd.DataFrame) -> pd.DataFrame:
    # Cast numeric fields
    numeric_cols = [
        "temp_max", "temp_min", "temp_avg",
        "precipitation", "rain", "snow", "pop",
        "wind_speed", "wind_deg", "clouds", "humidity"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Cast dates
    if "forecast_date" in df.columns:
        df["forecast_date"] = pd.to_datetime(df["forecast_date"], errors="coerce").dt.date

    if "forecast_time" in df.columns:
        df["forecast_time"] = pd.to_datetime(df["forecast_time"], errors="coerce", utc=True)

    # Cast sunrise/sunset
    time_cols = ["sunrise", "sunset"]
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.time

    # Strings
    string_cols = ["api_name", "place_name", "weather_main", "weather_description", "weather_icon"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    return df





















# def validate_silver_df(df: pd.DataFrame) -> pd.DataFrame:
#     logger = get_logger()
#
#     if df is None or df.empty:
#         logger.warning("Validation skipped: empty dataframe")
#         return df
#
#     schema = DataFrameSchema(
#         {
#             # identifiers
#             "api_name": Column(str, nullable=False),
#             "place_name": Column(str, nullable=False),
#
#             # time
#             "event_dt": Column(pa.DateTime, nullable=False),
#
#             # weather metrics
#             "temp": Column(
#                 float,
#                 nullable=True,
#                 checks=Check.between(-80, 70),
#             ),
#             "humidity": Column(
#                 float,
#                 nullable=True,
#                 checks=Check.between(0, 100),
#             ),
#             "wind_speed": Column(
#                 float,
#                 nullable=True,
#                 checks=Check.ge(0),
#             ),
#             "uv_index": Column(
#                 float,
#                 nullable=True,
#                 checks=Check.between(0, 15),
#             ),
#
#             # optional descriptive fields
#             "weather_main": Column(str, nullable=True),
#             "weather_desc": Column(str, nullable=True),
#         },
#         strict=False,  # allow extra columns
#         coerce=True,  # auto-cast types
#     )
#
#     try:
#         validated_df = schema.validate(df, lazy=True)
#
#         logger.info(
#             "Silver validation passed",
#             extra={"rows": len(validated_df)}
#         )
#
#         return validated_df
#
#     except pa.errors.SchemaErrors as e:
#         error_df = e.failure_cases
#
#         logger.error(
#             "Silver validation failed",
#             extra={
#                 "error_count": len(error_df),
#                 "errors": error_df.head(20).to_dict()
#             }
#         )
#
#         # drop bad rows
#         valid_index = set(df.index) - set(error_df["index"])
#         cleaned_df = df.loc[list(valid_index)].reset_index(drop=True)
#
#         logger.warning(
#             "Invalid rows removed",
#             extra={
#                 "rows_before": len(df),
#                 "rows_after": len(cleaned_df),
#                 "removed": len(df) - len(cleaned_df),
#             }
#         )
#
#         return cleaned_df

#
# def parse_bronze_df(api_name: str, api_df: pd.DataFrame, api_parsing: dict):
#     parser = api_parsing.get(api_name)
#     if not parser:
#         return None
#
#     parsed_frames = []
#
#     for _, row in api_df.iterrows():
#         df = parser(row["payload"])  # parser returns DataFrame
#         df["place_name"] = row["place_name"]
#         df["api_name"] = api_name
#         df["ingest_date"] = row["ingest_date"]
#         df["ingest_hour"] = row["ingest_hour"]
#         parsed_frames.append(df)
#
#     if parsed_frames:
#         return pd.concat(parsed_frames, ignore_index=True)
#     return None
