"""Postgres helpers shared by the Python jobs: connection, bucket upsert, ledger read."""

from __future__ import annotations

import os
from datetime import date
from typing import Iterable

import psycopg
from psycopg.rows import dict_row

UPSERT_BUCKET = """
    INSERT INTO usage_buckets (account_id, meter, hour, quantity, event_count)
    VALUES (%(account_id)s, %(meter)s, %(hour)s, %(quantity)s, %(event_count)s)
    ON CONFLICT (account_id, meter, hour) DO UPDATE
    SET quantity = EXCLUDED.quantity,
        event_count = EXCLUDED.event_count,
        updated_at = now()
"""

LEDGER_SQL = """
    SELECT account_id, meter, hour, quantity, amount, rate_version
    FROM ledger_lines
    WHERE hour >= %(day)s::date AND hour < %(day)s::date + interval '1 day'
    ORDER BY account_id, meter, hour
"""


def connect() -> psycopg.Connection:
    """Opens a connection from DATABASE_URL, defaulting to a local database."""
    dsn = os.environ.get(
        "DATABASE_URL", "postgresql://ledger:ledger@localhost:5432/ledger"
    )
    return psycopg.connect(dsn, row_factory=dict_row)


def upsert_buckets(conn: psycopg.Connection, rows: Iterable[dict]) -> int:
    """Writes hourly buckets, replacing the quantity when a batch is replayed."""
    written = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(UPSERT_BUCKET, row)
            written += 1
    conn.commit()
    return written


def fetch_ledger(conn: psycopg.Connection, day: date) -> list[dict]:
    """Returns the rated ledger lines for one UTC day."""
    with conn.cursor() as cur:
        cur.execute(LEDGER_SQL, {"day": day})
        return list(cur.fetchall())
