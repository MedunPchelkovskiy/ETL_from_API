import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from prefect import flow

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.tasks.gold.extract_from_silver import get_silver_parquet_azure, get_silver_data_postgres
from src.tasks.gold.load_gold_data import load_gold_data_to_azure, load_gold_data_to_postgres
from src.tasks.gold.transform_silver_data import get_daily_forecast_data


@flow(
    name="daily_dataset_forecast", )
def daily_forecast():
    logger = get_logger()

    now = pendulum.now("UTC")
    date = now.format("YYYY-MM-DD")
    hour_str = now.format("HH")

    logger.info(f"Starting flow daily dataset forecast",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "utc_time": now.to_iso8601_string(),
                })
    silver_df = None
    try:
        silver_df = get_silver_parquet_azure()
    except ResourceNotFoundError as e:
        logger.info(f"No parquet file found for {date}-{hour_str}, fall back to postgres | error={e}",
                    extra={
                        "flow_run_id": prefect.runtime.flow_run.id,
                        "task_run_id": prefect.runtime.task_run.id
                    })
        silver_df = get_silver_data_postgres()

    sample = silver_df.to_dict(orient="records")  # print in logs only during dev
    logger.info(f"End of flow, sample data: {sample}")  # print in logs only during dev

    df_data = get_daily_forecast_data(silver_df)
    load_gold_data_to_azure(df_data)
    # load_gold_data_to_postgres(df_data)

    # sample = df_data.to_dict(orient="records")              # print in logs only during dev
    # logger.info(f"End of flow, sample data: {df_data}")        # print in logs only during dev
    # logger.info("My list: %s", df_data)     # print in logs only during dev
    # logger.info("\n%s", df_data.head(301).to_string())    # print in logs only during dev
    for i in range(min(5, len(df_data))):  # print in logs only during dev
        logger.info("\nRow %s vertical:\n%s", i,
                    df_data.iloc[i].to_frame().to_string())  # print in logs only during dev

    logger.info(f"End flow daily dataset forecast",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "rows_loaded": len(df_data),
                    "utc_time": pendulum.now("UTC").to_iso8601_string()
                }
                )

    return df_data


if __name__ == "__main__":
    daily_forecast()
