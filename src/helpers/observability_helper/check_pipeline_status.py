import psycopg2
from datetime import datetime

from decouple import config


def try_acquire_pipeline_lock(pipeline_name: str, run_id: str) -> bool:
    conn = psycopg2.connect(
        host=config("DB_HOST"),
        dbname=config("DB_NAME_FOR_RAW_WEATHER_API_DATA"),
        user=config("DB_USER"),
        password=config("DB_PASSWORD")
    )

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (
                    run_id,
                    pipeline_name,
                    status,
                    trigger_type,
                    environment,
                    started_at,
                    updated_at
                )
                VALUES (%s, %s, 'processing', 'orchestrator', 'local', NOW(), NOW())
                ON CONFLICT (pipeline_name)
                DO NOTHING
                RETURNING run_id;
            """, (run_id, pipeline_name))

            return cur.fetchone() is not None

    finally:
        conn.commit()
        conn.close()