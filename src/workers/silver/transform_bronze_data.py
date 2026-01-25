# from src.helpers.silver.parsing_bronze_mapper import api_parsers

import pandas as pd

from src.workers.silver.generic_parser_bronze_data import parse_api_df


def parse_bronze_df(api_name: str, api_df: pd.DataFrame, api_paths: dict):
    """
    api_df = rows from ONE api only (because of groupby)
    """

    # 1) Flatten JSON payloads
    df = pd.json_normalize(api_df["payload"].tolist())

    # 2) Apply generic parser
    parsed = parse_api_df(df, api_name, api_paths)

    if parsed is None:
        return None

    # 3) Add metadata
    parsed["api_name"] = api_name
    parsed["place_name"] = api_df["place_name"].values
    parsed["ingest_date"] = api_df["ingest_date"].values
    parsed["ingest_hour"] = api_df["ingest_hour"].values

    return parsed


def clean_silver_df(df: pd.DataFrame):
    if df is None or df.empty:
        return df

    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    # example rules
    if "temperature" in df.columns:
        df = df[df["temperature"].notna()]  # TODO: add all df columns for cleaning

    return df


def validate_silver_df(df: pd.DataFrame):
    pass

    # TODO: add buisness logic to validate fields with pandera!

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
