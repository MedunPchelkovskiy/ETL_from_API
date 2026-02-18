import pendulum
import prefect
from azure.core.exceptions import ResourceNotFoundError
from prefect import flow

from src.helpers.gold.transform import flatten_incremental_silver
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.tasks.gold.extract_from_silver import get_silver_parquet_azure
from src.tasks.gold.load_gold_data import load_gold_data_to_azure, load_gold_daily_data_to_postgres
from src.tasks.gold.transform_silver_data import get_daily_forecast_data




@flow(
    name="daily_dataset_forecast", )
def daily_forecast(forecast_day=None, max_hour=None):  # possibly can pass old date and hour to ingest data
    logger = get_logger()
    now = pendulum.now("UTC")
    generated_at = now

    if forecast_day is None:
        forecast_day = now

    if max_hour is None:
        max_hour = now

    logger.info(f"Starting flow daily dataset forecast",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "utc_time": now.to_iso8601_string(),
                })

    silver_result = None
    try:
        # task вече връща list of tuples (hour, df) за пропуснатите часове
        silver_result = get_silver_parquet_azure(forecast_day, max_hour)
    except ResourceNotFoundError as e:
        logger.info(
            f"No parquet file found for day  {forecast_day.format('DD')}/{max_hour.hour}.parquet, fall back to postgres | error={e}",
            extra={
                "flow_run_id": prefect.runtime.flow_run.id,
                "task_run_id": prefect.runtime.task_run.id
            })
        # silver_result = get_silver_data_postgres(forecast_day, max_hour)  # закоментирано за момента

    # ✅ Flatten резултата в един DataFrame
    silver_df = flatten_incremental_silver(silver_result)

    if silver_df.empty:
        logger.info("No new Silver data to process after flattening.")
    else:
        sample = silver_df.to_dict(orient="records")  # print in logs only during dev
        logger.info(f"End of flow, sample data: {sample}")  # print in logs only during dev

    df_data = get_daily_forecast_data(silver_df, generated_at)
    load_gold_data_to_azure(df_data)
    load_gold_daily_data_to_postgres(df_data)

    # print first 5 rows vertically for dev logs
    for i in range(min(5, len(df_data))):
        logger.info("\nRow %s vertical:\n%s", i,
                    df_data.iloc[i].to_frame().to_string())

    logger.info(f"End flow daily dataset forecast",
                extra={
                    "flow_run_id": prefect.runtime.flow_run.id,
                    "task_run_id": prefect.runtime.task_run.id,
                    "rows_loaded": len(df_data),
                    "utc_time": pendulum.now("UTC").to_iso8601_string()
                })

    return df_data


if __name__ == "__main__":
    daily_forecast()













# @flow(
#     name="daily_dataset_forecast", )
# def daily_forecast(forecast_day=None, max_hour=None):  # possibly can pass old date and hour to ingest data
#     logger = get_logger()
#
#     now = pendulum.now("UTC")
#
#     generated_at = now
#
#     if forecast_day is None:
#         forecast_day = now
#
#     if max_hour is None:
#         max_hour = now
#
#     logger.info(f"Starting flow daily dataset forecast",
#                 extra={
#                     "flow_run_id": prefect.runtime.flow_run.id,
#                     "task_run_id": prefect.runtime.task_run.id,
#                     "utc_time": now.to_iso8601_string(),
#                 })
#     silver_df = None
#     try:
#         silver_df = get_silver_parquet_azure(forecast_day, max_hour)
#     except ResourceNotFoundError as e:
#         logger.info(
#             f"No parquet file found for day  {forecast_day.format("DD")}/{max_hour.hour}.parquet, fall back to postgres | error={e}",
#             extra={
#                 "flow_run_id": prefect.runtime.flow_run.id,
#                 "task_run_id": prefect.runtime.task_run.id
#             })
#         # silver_df = get_silver_data_postgres(forecast_day, max_hour)
#     if silver_df:
#         sample = silver_df.to_dict(orient="records")  # print in logs only during dev
#         logger.info(f"End of flow, sample data: {sample}")  # print in logs only during dev
#     logger.info(f"No silver_df after flow's try/except downloading block")  # print in logs only during dev
#
#     df_data = get_daily_forecast_data(silver_df, generated_at)
#     load_gold_data_to_azure(df_data)
#     load_gold_daily_data_to_postgres(df_data)
#
#     # sample = df_data.to_dict(orient="records")              # print in logs only during dev
#     # logger.info(f"End of flow, sample data: {df_data}")        # print in logs only during dev
#     # logger.info("My list: %s", df_data)     # print in logs only during dev
#     # logger.info("\n%s", df_data.head(301).to_string())    # print in logs only during dev
#     for i in range(min(5, len(df_data))):  # print in logs only during dev
#         logger.info("\nRow %s vertical:\n%s", i,
#                     df_data.iloc[i].to_frame().to_string())  # print in logs only during dev
#
#     logger.info(f"End flow daily dataset forecast",
#                 extra={
#                     "flow_run_id": prefect.runtime.flow_run.id,
#                     "task_run_id": prefect.runtime.task_run.id,
#                     "rows_loaded": len(df_data),
#                     "utc_time": pendulum.now("UTC").to_iso8601_string()
#                 }
#                 )
#
#     return df_data
#
#
# if __name__ == "__main__":
#     daily_forecast()
