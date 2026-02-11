import json
import time
from datetime import datetime
from typing import Optional

import pandas as pd
from azure.core.exceptions import ResourceNotFoundError
from decouple import config
# from decouple import config
from prefect import flow

from logging_config import setup_logging
from metrics import FLOW_DURATION, PIPELINE_RUNNING
from pushgateway_utils import push_metrics_to_gateway
from src.clients.datalake_client import fs_client
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.helpers.silver.parsing_mapper import api_data_parsers
from src.tasks.silver.extract_from_bronze_layer_tasks import extract_bronze_data_from_postgres, \
    extract_bronze_data_from_azure_blob_task
from src.tasks.silver.load_silver_data import load_silver_data_to_postgres, load_silver_data_to_azure
from src.tasks.silver.transform_bronze_data_tasks import clean_silver


@flow(
    flow_run_name=lambda: f"Extract bronze data for transformation flow - {datetime.now().strftime('%d%m%Y-%H%M%S')}"
    # Lambda give dynamically timestamp on every flow execution
)
def transform_bronze_data(api_parsers: dict = api_data_parsers,
                          date: Optional[str] = None,
                          hour: Optional[int] = None,
                          base_dir=config("BASE_DIR_RAW"),
                          azure_fs_client=fs_client):
    setup_logging()
    logger = get_logger()
    start = time.time()
    flow_name = "Transform bronze data"
    PIPELINE_RUNNING.labels(flow_name).set(1)

    try:
        now = datetime.now()

        date = now.strftime("%Y-%m-%d")  # canonical
        hour_str = now.strftime("%H")  # Azure
        hour_int = int(hour_str)  # Postgres

        try:
            bronze_records = extract_bronze_data_from_azure_blob_task(azure_fs_client, base_dir, date, hour_str)
            logger.info(
                "Records:\n%s",
                json.dumps(bronze_records, indent=2, ensure_ascii=False)
            )

        except ResourceNotFoundError as e:
            logger.warning(
                "Azure file not found, falling back to Postgres | error=%s", e
            )
            bronze_records = extract_bronze_data_from_postgres(date, hour_int)

        silver_parts = []

        for record in bronze_records:
            source = str(record.get("source", "")).strip()  # ensure string
            parser = api_parsers.get(source)

            if parser:
                # Wrap single dict in DataFrame for parser
                df_parsed = parser(record)
                if df_parsed is not None and not df_parsed.empty:
                    silver_parts.append(df_parsed)
            else:
                logger.warning("No parser found for source: %r", source)

        silver_df = pd.concat(silver_parts, ignore_index=True) if silver_parts else pd.DataFrame()
        # Step 4: clean
        silver_df = clean_silver(silver_df)

        logger.info("Final silver_df preview:\n%s", silver_df.head(301).to_string())

        # for i in range(min(5, len(silver_df))):
        #     logger.info("\nRow %s vertical:\n%s", i, silver_df.iloc[i].to_frame().to_string())

        # # Step 5: validate
        # valid = validate_silver_data(silver_df)
        #     # Step 6: load

        load_silver_data_to_azure(silver_df)
        load_silver_data_to_postgres(silver_df)

        status = "success"
    except Exception:
        status = "failed"
        raise
    finally:
        duration = time.time() - start
        # update FLOW_DURATION and PIPELINE_RUNNING for local use
        FLOW_DURATION.labels(flow_name).observe(duration)
        PIPELINE_RUNNING.labels(flow_name).set(0)

        # Push all metrics to Pushgateway
        push_metrics_to_gateway(flow_name=flow_name, status=status, duration=duration)


if __name__ == "__main__":
    transform_bronze_data()
