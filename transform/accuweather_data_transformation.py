import math

import pandas as pd

from helpers.transformation import split_date
from validation.accuweather_input_data_validation import validate_input_accuweather_data
from validation.accuweather_output_data_validation import validate_output_accuweather_data


def accuweather_transformation(df: pd.DataFrame):

    accuweather_df = validate_input_accuweather_data(df)

    accuweather_df[['weekday', 'curr_day', 'curr_month']] = df['curr_day'].apply(lambda x: pd.Series(split_date(x)))
    accuweather_df['city'] = df['city'].str.split(',').str[0]
    accuweather_df["temperature"] = df["temperature"].astype("int64")
    accuweather_df["wind_m_s"] = math.floor(int(df["wind_km_h"]) * 0.2777)
    accuweather_df.drop(columns=["wind_km_h"], inplace=True)

    accuweather_df = validate_output_accuweather_data(accuweather_df)

    return accuweather_df




