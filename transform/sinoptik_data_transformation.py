import pandas as pd

from helpers.transformation import split_date
from validation.sinoptik_input_data_validation import validate_input_sinoptik_data
from validation.sinoptik_output_data_validation import validate_output_sinoptik_data


def sinoptik_transformation(sinoptik_df: pd.DataFrame):
    sinoptik_df = validate_input_sinoptik_data(sinoptik_df)

    sinoptik_df[['curr_day', 'curr_month']] = sinoptik_df['curr_day'].apply(lambda x: pd.Series(split_date(x)))
    sinoptik_df["temperature"] = (sinoptik_df["temperature"]).astype("int64")
    sinoptik_df["wind_m_s"] = (sinoptik_df["wind_m_s"]).astype(int)

    sinoptik_df = validate_output_sinoptik_data(sinoptik_df)

    return sinoptik_df
