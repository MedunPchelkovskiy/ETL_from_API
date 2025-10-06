import pandas as pd

from helpers.transformation import split_date


def accuweather_transformation(accuweather_df: pd.DataFrame):

    accuweather_df[['weekday', 'curr_day', 'curr_month']] = accuweather_df['curr_day'].apply(lambda x: pd.Series(split_date(x)))
    accuweather_df['city'] = accuweather_df['city'].str.split(',').str[0]

    return accuweather_df




