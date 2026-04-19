"""FastAPI service for health checks and currency rate queries.

This module exposes API endpoints backed by a PostgreSQL database.
"""

import os
from contextlib import closing

import psycopg2
from fastapi import FastAPI, HTTPException


app = FastAPI(title="Benni Consumer Service", version="1.0.0")


def get_connection_benni():
    """Create and return a PostgreSQL connection using environment variables."""
    return psycopg2.connect(
        dbname=os.getenv("BENNI_DB_NAME"),
        user=os.getenv("BENNI_DB_USER"),
        password=os.getenv("BENNI_DB_PASSWORD"),
        host=os.getenv("BENNI_DB_HOST", "benni-postgres"),
        port=int(os.getenv("BENNI_DB_PORT", "5432")),
    )


def get_connection_felix():
    """Create and return a PostgreSQL connection for Felix's database."""
    return psycopg2.connect(
        dbname=os.getenv("FELIX_DB_NAME"),
        user=os.getenv("FELIX_DB_USER"),
        password=os.getenv("FELIX_DB_PASSWORD"),
        host=os.getenv("FELIX_DB_HOST", "felix-postgres"),
        port=int(os.getenv("FELIX_DB_PORT", "5432")),
    )


@app.get(
    "/health", summary="Health check", description="Verifies database connectivity."
)
def health_check():
    """Return service status by running a simple query against the database."""

    # Check on Benni's database
    try:
        with closing(get_connection_benni()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

    except Exception as exc:
        raise HTTPException(
            status_code=503, detail=f"Benni Database unavailable: {exc}"
        )
    # Check on Felix's database
    try:
        with closing(get_connection_felix()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except Exception as exc:
        raise HTTPException(
            status_code=503, detail=f"Felix Database unavailable: {exc}"
        )

    return {"status": "ok", "benni_db": "available", "felix_db": "available"}


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
        with closing(get_connection_benni()) as conn:
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
        raise HTTPException(
            status_code=500, detail=f"Benni Database query failed: {exc}"
        )


def get_currency_last_four_months_data(currency: str):
    if currency.strip().upper() != "USDT":
        """Get all records for a currency from the last four months."""
        query = """
        SELECT id, currency, rate, scraped_at
        FROM currency_rates
        WHERE UPPER(currency) = UPPER(%s)
            AND scraped_at >= (CURRENT_DATE - INTERVAL '4 months')
        ORDER BY scraped_at DESC;
        """

        try:
            with closing(get_connection_benni()) as conn:
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
            raise HTTPException(
                status_code=500, detail=f"Benni Database query failed: {exc}"
            )
    else:
        query = """
        SELECT id, rate_usdt_to_bs, scraped_at
        FROM exchange_rates
        WHERE scraped_at >= (CURRENT_DATE - INTERVAL '4 months')
        ORDER BY scraped_at DESC;"""

        try:
            with closing(get_connection_felix()) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (currency,))
                    rows = cur.fetchall()

            return {
                "currency": currency.upper(),
                "count": len(rows),
                "rows": [
                    {
                        "id": row[0],
                        "currency": "usdt",
                        "rate": row[1],
                        "scraped_at": row[2].isoformat() if row[2] else None,
                    }
                    for row in rows
                ],
            }
        except Exception as exc:
            raise HTTPException(
                status_code=500, detail=f"Felix Database query failed: {exc}"
            )


@app.get(
    "/currency-rates/last-four-months/{currency}",
    summary="Currency rates from the last four months",
    description="Returns all records for the requested currency over the last four months.",
)
def currency_last_four_months(currency: str):
    """API endpoint wrapper for last-four-month currency data retrieval."""
    return get_currency_last_four_months_data(currency)
