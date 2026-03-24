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


def latest_per_book(
    conn: sqlite3.Connection,
    *,
    event_key: str,
    participant_key: str,
    before_utc: str,
) -> list[Quote]:
    """Latest row per book at or before before_utc (ISO string compares lexicographically for Zulu)."""
    cur = conn.execute(
        """
        SELECT o.book, o.ts_utc, o.american_odds, o.line
        FROM odds_snapshot o
        INNER JOIN (
          SELECT book, MAX(ts_utc) AS ts
          FROM odds_snapshot
          WHERE event_key = ? AND participant_key = ? AND ts_utc <= ?
          GROUP BY book
        ) t ON o.book = t.book AND o.ts_utc = t.ts
        WHERE o.event_key = ? AND o.participant_key = ?
        ORDER BY o.book
        """,
        (event_key, participant_key, before_utc, event_key, participant_key),
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
