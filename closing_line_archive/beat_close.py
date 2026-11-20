"""Compare open vs close snapshots per book."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass
class Quote:
    book: str
    ts_utc: str
    american_odds: int | None
    line: float | None


def resolve_market(conn, event_key, participant_key, before_utc):
    rows = conn.execute(
        "SELECT DISTINCT market_type FROM odds_snapshot "
        "WHERE event_key = ? AND participant_key = ? AND ts_utc <= ? LIMIT 2",
        (event_key, participant_key, before_utc),
    ).fetchall()
    if len(rows) > 1:
        raise ValueError("multiple markets match this participant; specify --market")
    return rows[0]["market_type"] if rows else None


def latest_per_book(
    conn: sqlite3.Connection,
    *,
    event_key: str,
    participant_key: str,
    before_utc: str,
    market_type: str | None = None,
) -> list[Quote]:
    """Latest row per book at or before before_utc (ISO string compares lexicographically for Zulu)."""
    if market_type is None:
        market_type = resolve_market(conn, event_key, participant_key, before_utc)
    cur = conn.execute(
        """
        SELECT book, ts_utc, american_odds, line
        FROM (
            SELECT book, ts_utc, american_odds, line,
                   ROW_NUMBER() OVER (PARTITION BY book ORDER BY ts_utc DESC, id DESC) AS rank
            FROM odds_snapshot
            WHERE event_key = ? AND participant_key = ? AND market_type = ? AND ts_utc <= ?
        )
        WHERE rank = 1
        ORDER BY book
        """,
        (event_key, participant_key, market_type, before_utc),
    )
    rows = cur.fetchall()
    return [Quote(r["book"], r["ts_utc"], r["american_odds"], r["line"]) for r in rows]


def implied_from_american(odds: int) -> float:
    o = float(odds)
    if o > 0:
        return 100.0 / (o + 100.0)
    return abs(o) / (abs(o) + 100.0)


def price_rank(american_odds: int) -> float:
    """Higher = better for bettor on a standard −110 style line (more positive American = better)."""
    return float(american_odds)
