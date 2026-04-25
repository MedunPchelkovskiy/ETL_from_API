# from datetime import datetime
# import psycopg2
#
#
# def set_pipeline_status(
#     pipeline_name: str,
#     status: str,
#     run_id: str,
#     error: str = None
# ):
#     conn = psycopg2.connect(
#         host="YOUR_HOST",
#         dbname="YOUR_DB",
#         user="YOUR_USER",
#         password="YOUR_PASSWORD"
#     )
#
#     try:
#         with conn.cursor() as cur:
#             cur.execute("""
#                 INSERT INTO pipeline_runs (
#                     pipeline_name,
#                     status,
#                     run_id,
#                     updated_at,
#                     error_message
#                 )
#                 VALUES (%s, %s, %s, %s, %s)
#                 ON CONFLICT (pipeline_name)
#                 DO UPDATE SET
#                     status = EXCLUDED.status,
#                     run_id = EXCLUDED.run_id,
#                     updated_at = EXCLUDED.updated_at,
#                     error_message = EXCLUDED.error_message;
#             """, (
#                 pipeline_name,
#                 status,
#                 run_id,
#                 datetime.utcnow(),
#                 error
#             ))
#
#         conn.commit()
#
#     finally:
#         conn.close()




import datetime
import os
import psycopg2
from decouple import config


def set_pipeline_status(
    pipeline_name: str,
    status: str,
    run_id: str,
    error: str = None,
    trigger_type: str = "orchestrator",
    environment: str = None,
):
    conn = psycopg2.connect(
        host=config("DB_HOST"),
        dbname=config("DB_NAME_FOR_RAW_WEATHER_API_DATA"),
        user=config("DB_USER"),
        password=config("DB_PASSWORD")
    )

    try:
        env = environment or os.getenv("ENV", "local")
        now = datetime.datetime.now(datetime.UTC)

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (
                    run_id,
                    pipeline_name,
                    status,
                    trigger_type,
                    environment,
                    started_at,
                    finished_at,
                    duration_seconds,
                    updated_at,
                    error_message
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, NULL, NULL, %s, %s
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at,
                    finished_at = CASE
                        WHEN EXCLUDED.status IN ('success', 'failed', 'cancelled')
                        THEN EXCLUDED.updated_at
                        ELSE pipeline_runs.finished_at
                    END,
                    duration_seconds = CASE
                        WHEN EXCLUDED.status IN ('success', 'failed', 'cancelled')
                        THEN EXTRACT(EPOCH FROM (EXCLUDED.updated_at - pipeline_runs.started_at))::INT
                        ELSE pipeline_runs.duration_seconds
                    END,
                    error_message = EXCLUDED.error_message;
            """, (
                run_id,
                pipeline_name,
                status,
                trigger_type,
                env,
                now,
                now,
                error
            ))

        conn.commit()

    finally:
        conn.close()