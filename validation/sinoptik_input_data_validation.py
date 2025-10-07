import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column

sinoptik_input_data_schema = pa.DataFrameSchema({
        "city": Column(str),
        "curr_day": Column(str),
        "curr_time": Column(str),
        "temperature": Column(str),
        "weather": Column(str),
        "wind_m_s": Column(str),
})


def validate_input_sinoptik_data(df: pd.DataFrame):
    return sinoptik_input_data_schema.validate(df)