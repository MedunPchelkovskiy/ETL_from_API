from datetime import datetime
from typing import Optional

import pandas as pd
# from decouple import config
from prefect import flow

from src.helpers.logging_helper.combine_loggers_helper import get_logger
from src.helpers.silver.parsing_mapper import api_parsers
from src.helpers.silver.source_naming_mapper import canonical_api_map
from src.tasks.silver.extract_from_bronze_layer_tasks import extract_bronze_data
from src.tasks.silver.load_to_gold_tasks import load_silver_data_to_postgres_task
from src.tasks.silver.transform_bronze_data_tasks import parse_api_group, clean_silver


# from sqlalchemy import create_engine


@flow(
    flow_run_name=lambda: f"extract_bronze_data_from_local_postgres - {datetime.now().strftime('%d%m%Y-%H%M%S')}"
    # Lambda give dynamically timestamp on every flow execution
)
def transform_bronze_data(api_parsing: dict = api_parsers, date: Optional[str] = None,
                          hour: Optional[int] = None):
    logger = get_logger()

    now = datetime.now()
    if date is None:
        date = now.strftime("%Y-%m-%d")
    if hour is None:
        hour = now.hour

    # Step 1: fetch bronze
    bronze_df = extract_bronze_data(date, hour)

    silver_parts = []

    # Step 2: group in-memory
    for raw_api_name, api_df in bronze_df.groupby("source"):
        logger.info("Parsing API %s | Rows: %s", raw_api_name, len(api_df))
        try:
            api_name = canonical_api_map[raw_api_name]
        except KeyError:
            logger.warning(
                "Unknown raw API source, skipping | raw_name=%s", raw_api_name
            )
            api_name = None
        parsed = parse_api_group(api_name, api_df, api_parsing)

        if parsed is not None and not parsed.empty:
            silver_parts.append(parsed)
            logger.info("Parsed rows: %s", len(parsed))
        else:
            logger.warning("Parser returned empty for API: %s", api_name)

    # Step 3: merge all
    if not silver_parts:
        logger.warning("No silver data produced from any API")
        silver_df = pd.DataFrame()
    else:
        silver_df = pd.concat(silver_parts, ignore_index=True)

    # Step 4: clean
    silver_df = clean_silver(silver_df)

    logger.info("Final silver_df preview:\n%s", silver_df.head(301).to_string())

    # for i in range(min(5, len(silver_df))):
    #     logger.info("\nRow %s vertical:\n%s", i, silver_df.iloc[i].to_frame().to_string())

    # # Step 5: validate
    # valid = validate_silver_data(silver_df)
    #     # Step 6: load

    load_silver_data_to_postgres_task(silver_df)


if __name__ == "__main__":
    transform_bronze_data()
