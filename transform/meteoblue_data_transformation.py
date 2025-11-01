import pandas as pd

from validation.meteoblue_input_data_validation import validate_meteoblue_input_data
from validation.meteoblue_output_data_validation import validate_meteoblue_output_data


def meteoblue_transformation(df: pd.DataFrame):

    meteoblue_df = validate_meteoblue_input_data(df)

    meteoblue_df["temperature"] = meteoblue_df["temperature"].astype("int64")

    return validate_meteoblue_output_data(meteoblue_df)