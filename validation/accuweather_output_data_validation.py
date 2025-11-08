import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column

accuweather_output_data_schema = pa.DataFrameSchema({
        "city": Column(str),
        "curr_time": Column(str),
        "temperature": Column(int),
        "weather": Column(str),
        "weekday": Column(str),
        "wind_m_s": Column(int),
        "curr_day": Column(int),
        "curr_month": Column(str),
})


def validate_output_accuweather_data(df: pd.DataFrame):
    return accuweather_output_data_schema.validate(df)