import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column

accuweather_input_data_schema = pa.DataFrameSchema({
        "city": Column(str),
        "curr_day": Column(str),
        "curr_time": Column(str),
        "temperature": Column(str),
        "weather": Column(str),
        "wind_km_h": Column(str),
})


def validate_input_accuweather_data(df: pd.DataFrame):
    return accuweather_input_data_schema.validate(df)