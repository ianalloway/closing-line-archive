"""SQLite schema and helpers."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS odds_snapshot (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_utc TEXT NOT NULL,
  book TEXT NOT NULL,
  market_type TEXT NOT NULL,
  event_key TEXT NOT NULL,
  participant_key TEXT NOT NULL,
  american_odds INTEGER,
  line REAL,
  implied_prob REAL,
  raw_json TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_odds_event_ts ON odds_snapshot(event_key, ts_utc);
CREATE INDEX IF NOT EXISTS idx_odds_event_participant ON odds_snapshot(event_key, participant_key);
"""


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(path: Path) -> None:
    with connect(path) as conn:
        conn.executescript(SCHEMA)


def insert_row(
    conn: sqlite3.Connection,
    *,
    ts_utc: str,
    book: str,
    market_type: str,
    event_key: str,
    participant_key: str,
    american_odds: int | None,
    line: float | None,
    implied_prob: float | None,
    raw: dict[str, Any] | None,
) -> None:
    raw_json = json.dumps(raw, sort_keys=True) if raw is not None else None
    conn.execute(
        """
        INSERT INTO odds_snapshot (
          ts_utc, book, market_type, event_key, participant_key,
          american_odds, line, implied_prob, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts_utc,
            book.lower().strip(),
            market_type.lower().strip(),
            event_key.strip(),
            participant_key.strip(),
            american_odds,
            line,
            implied_prob,
            raw_json,
        ),
    )


def parse_line_obj(obj: dict[str, Any]) -> dict[str, Any]:
    required = ("ts_utc", "book", "market_type", "event_key", "participant_key")
    for k in required:
        if k not in obj:
            raise ValueError(f"missing field: {k}")
    return {
        "ts_utc": str(obj["ts_utc"]),
        "book": str(obj["book"]),
        "market_type": str(obj["market_type"]),
        "event_key": str(obj["event_key"]),
        "participant_key": str(obj["participant_key"]),
        "american_odds": int(obj["american_odds"]) if obj.get("american_odds") is not None else None,
        "line": float(obj["line"]) if obj.get("line") is not None else None,
        "implied_prob": float(obj["implied_prob"]) if obj.get("implied_prob") is not None else None,
        "raw": obj,
    }
