import os
from contextlib import closing

import psycopg2
from fastapi import FastAPI, HTTPException


app = FastAPI(title="Benni Consumer Service", version="1.0.0")


def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("BENNI_DB_NAME"),
        user=os.getenv("BENNI_DB_USER"),
        password=os.getenv("BENNI_DB_PASSWORD"),
        host=os.getenv("BENNI_DB_HOST", "benni-postgres"),
        port=int(os.getenv("BENNI_DB_PORT", "5432")),
    )


@app.get("/health")
def health_check():
    try:
        with closing(get_connection()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


@app.get("/currency-rates/first-five")
def first_five_rows():
    query = """
    SELECT id, currency, rate, scraped_at
    FROM currency_rates
    ORDER BY id ASC
    LIMIT 5;
    """

    try:
        with closing(get_connection()) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

        return {
            "count": len(rows),
            "rows": [
                {
                    "id": row[0],
                    "currency": row[1],
                    "rate": row[2],
                    "scraped_at": row[3].isoformat() if row[3] else None,
                }
                for row in rows
            ],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Query failed: {exc}")
