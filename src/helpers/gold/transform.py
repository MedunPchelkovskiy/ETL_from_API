def flatten_incremental_silver(result_list):
    """
    result_list: list of tuples (hour, df)
    връща pd.DataFrame с всички редове обединени
    """
    if not result_list:
        return pd.DataFrame()
    return pd.concat([df for _, df in result_list], ignore_index=True)