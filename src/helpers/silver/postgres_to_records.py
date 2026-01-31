import pandas as pd


def postgres_to_records(raw_df: pd.DataFrame) -> list[dict]:
    return [
        {
            "payload": row["payload"],   # still JSON
            "source": row["source"],
            "place_name": row["place_name"],
            "ingest_date": row["ingest_date"],
            "ingest_hour": row["ingest_hour"]
        }
        for _, row in raw_df.iterrows()
    ]
