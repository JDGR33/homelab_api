"""FastAPI service for health checks and currency rate queries.

This module exposes API endpoints backed by a PostgreSQL database.
"""

import os
from contextlib import closing

import psycopg2
from fastapi import FastAPI, HTTPException


app = FastAPI(title="Benni Consumer Service", version="1.0.0")


def get_connection():
    """Create and return a PostgreSQL connection using environment variables."""
    return psycopg2.connect(
        dbname=os.getenv("BENNI_DB_NAME"),
        user=os.getenv("BENNI_DB_USER"),
        password=os.getenv("BENNI_DB_PASSWORD"),
        host=os.getenv("BENNI_DB_HOST", "benni-postgres"),
        port=int(os.getenv("BENNI_DB_PORT", "5432")),
    )


@app.get(
    "/health", summary="Health check", description="Verifies database connectivity."
)
def health_check():
    """Return service status by running a simple query against the database."""
    try:
        with closing(get_connection()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


@app.get(
    "/currency-rates/first-five",
    summary="First five currency-rate rows",
    description="Returns the first five rows from the currency_rates table.",
)
def first_five_rows():
    """Fetch the first five rows from the currency_rates table."""
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


def get_currency_last_four_months_data(currency: str):
    """Get all records for a currency from the last four months."""
    query = """
    SELECT id, currency, rate, scraped_at
    FROM currency_rates
    WHERE UPPER(currency) = UPPER(%s)
      AND scraped_at >= (CURRENT_DATE - INTERVAL '4 months')
    ORDER BY scraped_at DESC;
    """

    try:
        with closing(get_connection()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (currency,))
                rows = cur.fetchall()

        return {
            "currency": currency.upper(),
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


@app.get(
    "/currency-rates/last-four-months/{currency}",
    summary="Currency rates from the last four months",
    description="Returns all records for the requested currency over the last four months.",
)
def currency_last_four_months(currency: str):
    """API endpoint wrapper for last-four-month currency data retrieval."""
    return get_currency_last_four_months_data(currency)
