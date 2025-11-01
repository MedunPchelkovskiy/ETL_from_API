import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column

meteoblue_input_data_schema = pa.DataFrameSchema({
    "city": Column(str),
    "curr_time": Column(str),
    "temperature": Column(str)
})


def validate_meteoblue_input_data(df: pd.DataFrame):
    return meteoblue_input_data_schema.validate(df)