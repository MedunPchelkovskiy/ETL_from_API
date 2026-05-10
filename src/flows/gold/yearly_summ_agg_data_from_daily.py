from prefect import flow


@flow(
    name="Aggregate daily to yearly flow")
def daily_to_yearly_aggregation():
    pass