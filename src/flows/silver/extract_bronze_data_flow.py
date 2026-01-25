from datetime import datetime
from typing import Optional

import pandas as pd
from decouple import config
from prefect import flow

from src.helpers.silver.parsing_bronze_mapper import api_parsers
from src.tasks.silver.extract_from_bronze_layer_tasks import \
    extract_bronze_data
from src.tasks.silver.load_to_gold_tasks import load_silver_data_to_postgres
from src.tasks.silver.transform_bronze_data_tasks import parse_api_group, validate_silver_data, clean_silver

connection_string = config("DB_CONN")


@flow(
    flow_run_name=lambda: f"extract_bronze_data_from_local_postgres - {datetime.now().strftime('%d%m%Y-%H%M%S')}"
    # Lambda give dynamically timestamp on every flow execution
)
def transform_bronze_data(conn=connection_string, api_parsing: dict = api_parsers, date: Optional[str] = None, hour: Optional[int] = None):
    now = datetime.now()
    if date is None:
        date = now.strftime("%Y-%m-%d")
    if hour is None:
        hour = now.hour
    # Step 1: fetch bronze
    bronze_df = extract_bronze_data(conn, date, hour)

    silver_parts = []

    # Step 2: group in-memory
    for api_name, api_df in bronze_df.groupby("source"):
        parsed = parse_api_group(api_name, api_df, api_parsing)
        silver_parts.append(parsed)

    # Step 3: merge all
    silver_df = pd.concat([df for df in silver_parts if df is not None], ignore_index=True)

    # Step 4: clean
    silver_df = clean_silver(silver_df)

    # Step 5: validate
    valid = validate_silver_data(silver_df)
    if valid:
        # Step 6: load
        load_silver_data_to_postgres(silver_df, conn)


if __name__ == "__main__":
    transform_bronze_data()
