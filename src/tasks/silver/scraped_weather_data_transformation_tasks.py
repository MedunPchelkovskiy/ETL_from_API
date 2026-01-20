import math

import pandas as pd

from src.helpers.silver.transformation import split_date
from src.validation.scraped_data.accuweather_input_data_validation import validate_input_accuweather_data
from src.validation.scraped_data.accuweather_output_data_validation import validate_output_accuweather_data
from src.validation.scraped_data.meteoblue_input_data_validation import validate_meteoblue_input_data
from src.validation.scraped_data.meteoblue_output_data_validation import validate_meteoblue_output_data
from src.validation.scraped_data.sinoptik_input_data_validation import validate_input_sinoptik_data
from src.validation.scraped_data.sinoptik_output_data_validation import validate_output_sinoptik_data


def accuweather_transformation(df: pd.DataFrame):
    accuweather_df = validate_input_accuweather_data(df)

    accuweather_df[['weekday', 'curr_day', 'curr_month']] = df['curr_day'].apply(lambda x: pd.Series(split_date(x)))
    accuweather_df['city'] = df['city'].str.split(',').str[0]
    accuweather_df["temperature"] = df["temperature"].astype("int64")
    accuweather_df["wind_m_s"] = math.floor(int(df["wind_km_h"].iloc[0]) * 0.2777)

    accuweather_df.drop(columns=["wind_km_h"], inplace=True)

    accuweather_df = validate_output_accuweather_data(accuweather_df)

    return accuweather_df


def meteoblue_transformation(df: pd.DataFrame):
    meteoblue_df = validate_meteoblue_input_data(df)

    meteoblue_df["temperature"] = meteoblue_df["temperature"].astype("int64")

    return validate_meteoblue_output_data(meteoblue_df)


def sinoptik_transformation(df: pd.DataFrame):
    sinoptik_df = validate_input_sinoptik_data(df)

    sinoptik_df[['curr_day', 'curr_month']] = sinoptik_df['curr_day'].apply(lambda x: pd.Series(split_date(x)))
    sinoptik_df["temperature"] = (sinoptik_df["temperature"]).astype("int64")
    sinoptik_df["wind_m_s"] = (sinoptik_df["wind_m_s"]).astype(int)

    sinoptik_df = validate_output_sinoptik_data(sinoptik_df)

    return sinoptik_df
