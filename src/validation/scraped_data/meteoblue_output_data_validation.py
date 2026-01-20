import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column

meteoblue_output_data_schema = pa.DataFrameSchema({
    "city": Column(str),
    "curr_time": Column(str),
    "temperature": Column(int)
})


def validate_meteoblue_output_data(df: pd.DataFrame):
    return meteoblue_output_data_schema.validate(df)