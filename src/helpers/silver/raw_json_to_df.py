import pandas as pd


# Convert JSON payload to DataFrame
def json_to_df(json_payload):
    return pd.json_normalize(json_payload)


# Combine multiple DataFrames
def combine_dfs(dfs):
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


# Compute per-file metrics
def compute_file_metrics(file_name, df, success=True, reason=None):
    return {
        "file_name": file_name,
        "rows": len(df) if df is not None else 0,
        "success": success,
        "reason": reason
    }
