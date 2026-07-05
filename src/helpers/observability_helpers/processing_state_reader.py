import psycopg2
from decouple import config


def get_processing_state_metrics(
        processing_level: str | None = None,
) -> list[dict]:
    """
    Взима последния ред (по partition_date) за всеки processing_level
    със status, completeness_ratio, retry_count, error_type, is_acceptable.
    """
    conn = psycopg2.connect(config("DB_CONN_RAW"))
    try:
        conditions = []
        params = []

        if processing_level:
            conditions.append("processing_level = %s")
            params.append(processing_level)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        query = f"""
            SELECT DISTINCT ON (processing_level)
                   processing_level, partition_date, status,
                   completeness_ratio, retry_count, error_type, is_acceptable
            FROM processing_state
            {where_clause}
            ORDER BY processing_level, partition_date DESC
        """

        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        return [
            {
                "processing_level": row[0],
                "partition_date": row[1],
                "status": row[2],
                "completeness_ratio": row[3],
                "retry_count": row[4],
                "error_type": row[5],
                "is_acceptable": row[6],
            }
            for row in rows
        ]
    finally:
        conn.close()