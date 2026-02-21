import pendulum
from fabric.decorators import task

from src.clients.datalake_client import fs_client
from src.helpers.gold.extract import get_last_processed_timestamp
from src.helpers.logging_helpers.combine_loggers_helper import get_logger
from src.workers.gold.extract_silver_data import fetch_silver_parquet_blob


@task(name="Get silver data from Azure Parquet Incremental", retries=3, retry_delay_seconds=60)
def get_silver_parquet_azure(forecast_day, max_hour):
    logger = get_logger()
    start_time = pendulum.now("UTC")

    pipeline_name = "daily_dataset_forecast"

    last_processed_ts = get_last_processed_timestamp(pipeline_name)
    logger.info("Last processed TS: {}".format(last_processed_ts))

    if last_processed_ts is None:
        last_processed_ts = (pendulum.now("UTC").start_of("hour").subtract(days=1))
        logger.info(f"If last processed is None.")

    target_ts = forecast_day.replace(
        hour=int(max_hour.format("HH")),
        minute=0,
        second=0,
        microsecond=0
    )

    if last_processed_ts >= target_ts:
        logger.info("No new Silver files to process.")
        return []

    result = []
    current_ts = last_processed_ts.start_of("hour").add(hours=1)

    while current_ts <= target_ts:

        year = current_ts.format("YYYY")
        month = current_ts.format("MM")
        day = current_ts.format("DD")
        hour = current_ts.format("HH")

        logger.info(f"Processing Silver {year}-{month}-{day} {hour}:00")

        try:
            df = fetch_silver_parquet_blob(
                year=year,
                month=month,
                day=day,
                hour=hour,
                fs_client=fs_client
            )

            if not df.empty:
                result.append((current_ts, df))
            else:
                logger.warning(f"Empty Silver file for {year}-{month}-{day} {hour}")

        except Exception as e:
            logger.warning(
                f"Missing Silver file for {year}-{month}-{day} {hour} | error={e}"
            )

        current_ts = current_ts.add(hours=1)

    duration = (pendulum.now("UTC") - start_time).total_seconds()

    logger.info(
        f"Finished incremental Silver download. "
        f"Windows processed: {len(result)}. "
        f"Duration: {duration:.2f}s"
    )

    return result

# @task(name="Get silver data from Azure Parquet Incremental", retries=3, retry_delay_seconds=60)
# def get_silver_parquet_azure(forecast_day, max_hour):
#     logger = get_logger()
#     start = pendulum.now("UTC")
#
#     year = forecast_day.format("YYYY")
#     month = forecast_day.format("MM")
#     day = forecast_day.format("DD")
#
#     dir_path = f"{config('BASE_DIR_GOLD')}/{year}/{month}/{day}"
#
#     # 1️ Проверяваме до кой час вече имаме Gold
#     last_processed_hour = get_last_processed_hour_from_gold(
#         fs_client,
#         dir_path
#     )
#
#     logger.info(f"Last processed Gold hour: {last_processed_hour}")
#
#     # 2️Определяме до кой час трябва да обработим (например текущия max_hour)
#     target_hour = int(max_hour.format("HH"))
#
#     if last_processed_hour >= target_hour:
#         logger.info("No new Silver files to process.")
#         return pd.DataFrame()
#
#     result = []
#
#     # Backlog loop
#     for hour in range(last_processed_hour + 1, target_hour + 1):
#
#         logger.info(f"Processing Silver hour {hour:02d}")
#
#         df = fetch_silver_parquet_blob(
#             year,
#             month,
#             day,
#             f"{hour:02d}",
#             fs_client
#         )
#
#         if df.empty:
#             logger.warning(f"Silver file missing or empty for {hour:02d}")
#             continue
#
#         result.append((hour, df))
#
#     duration = (pendulum.now("UTC") - start).total_seconds()
#
#     logger.info(
#         f"Finished incremental Silver download. "
#         f"Hours processed: {len(result)}. "
#         f"Duration: {duration:.2f}s"
#     )
#
#     return result

# @task(name="Get silver data from azure", retries=3, retry_delay_seconds=60)
# def get_silver_parquet_azure(forecast_day, max_hour):
#     logger = get_logger()
#     start_time = pendulum.now("UTC")
#
#     year, month, day, hour = (forecast_day.format("YYYY"), forecast_day.format("MM"),
#                               forecast_day.format("DD"), max_hour.format("HH"))
#     logger.info(f"Start task get silver parquet azure for {year}-{month}-{day} {hour}:00",
#                 extra={
#                     "flow_run_id": prefect.runtime.flow_run.id,
#                     "task_run_id": prefect.runtime.task_run.id,
#                 }
#                 )
#     try:
#         silver_parquet = fetch_silver_parquet_blob(year, month, day, hour, fs_client)
#         silver_df = pd.read_parquet(silver_parquet)
#     except FileNotFoundError:
#         logger.error(f"Silver parquet not found for {year}-{month}-{day}-{hour}")
#         raise
#     except Exception as e:
#         logger.error(f"Failed to get silver parquet: {e}")
#         raise
#
#     duration = (pendulum.now("UTC") - start_time).total_seconds()
#     logger.info(
#         "Finished task get silver parquet azure",
#         extra={
#             "flow_run_id": prefect.runtime.flow_run.id,
#             "task_run_id": prefect.runtime.task_run.id,
#             "rows_loaded": len(silver_df),
#             "duration_seconds": duration
#         }
#     )
#
#     return silver_df
#
#
# @task(name="daily forecast")
# def get_silver_data_postgres(forecast_day, max_hour):
#     logger = get_logger()
#
#     logger.info("Start task get silver data postgres",
#                 extra={
#                     "flow_run_id": prefect.runtime.flow_run.id,
#                     "task_run_id": prefect.runtime.task_run.id
#                 })
#     engine = create_engine(config("DB_CONN_RAW"))
#     silver_df = None
#     try:
#         silver_df = fetch_silver_data_postgres(forecast_day.format('YYYY-MM-DD'), max_hour.hour, engine)
#
#
#     except OperationalError as e:
#         logger.error(f"DB connection failed: {e}")
#
#     except DBAPIError as e:
#         logger.error(f"DB query error: {e}")
#
#     except SQLAlchemyError as e:
#         logger.error(f"SQLAlchemy error: {e}")
#
#     except Exception as e:
#         logger.error(f"Unexpected error: {e}")
#
#     end = pendulum.now("UTC")
#
#     logger.info(
#         "Finished task get silver data postgres",
#         extra={
#             "flow_run_id": prefect.runtime.flow_run.id,
#             "task_run_id": prefect.runtime.task_run.id,
#         }
#     )
#     return silver_df
