from datetime import datetime

from prefect import flow

from src.tasks.silver.extract_raw_weather_data_from_bronze_layer import \
    extract_bronze_data_for_transformation_from_postgres


@flow(
    flow_run_name=lambda: f"extract_bronze_data_from_local_postgres - {datetime.now().strftime('%d%m%Y-%H%M%S')}"
    # Lambda give dynamically timestamp on every flow execution
)
def transform_bronze_data():
    bronze_data = extract_bronze_data_for_transformation_from_postgres()
    print(bronze_data)

if __name__ == "__main__":
    transform_bronze_data()
