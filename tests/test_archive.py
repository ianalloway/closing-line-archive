from __future__ import annotations

import argparse
import io
import json
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from closing_line_archive.beat_close import latest_per_book
from closing_line_archive.cli import cmd_beat_close, main
from closing_line_archive.db import connect, init_db, insert_row


def test_init_db_row(tmp_path: Path):
    db = tmp_path / "o.sqlite"
    init_db(db)
    with connect(db) as conn:
        insert_row(
            conn,
            ts_utc="2026-03-01T12:00:00Z",
            book="dk",
            market_type="spreads",
            event_key="ev1",
            participant_key="LAL",
            american_odds=-110,
            line=7.5,
            implied_prob=None,
            raw=None,
        )
        conn.commit()
    with connect(db) as conn:
        cur = conn.execute("SELECT COUNT(*) AS c FROM odds_snapshot")
        assert cur.fetchone()["c"] == 1


def test_beat_close_flow(tmp_path: Path):
    db = tmp_path / "o.sqlite"
    init_db(db)
    rows = [
        ("2026-03-01T10:00:00Z", "dk", -115),
        ("2026-03-01T18:00:00Z", "dk", -110),
        ("2026-03-01T10:00:00Z", "fd", -108),
        ("2026-03-01T18:00:00Z", "fd", -112),
    ]
    with connect(db) as conn:
        for ts, book, am in rows:
            insert_row(
                conn,
                ts_utc=ts,
                book=book,
                market_type="spreads",
                event_key="ev1",
                participant_key="LAL",
                american_odds=am,
                line=7.5,
                implied_prob=None,
                raw=None,
            )
        conn.commit()

    ns = argparse.Namespace(
        db=str(db),
        event="ev1",
        participant="LAL",
        open_before="2026-03-01T12:00:00Z",
        close_before="2026-03-01T20:00:00Z",
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_beat_close(ns)
    out = buf.getvalue()
    assert "dk" in out and "fd" in out
    assert "yes" in out and "no" in out


def test_append_jsonl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db = tmp_path / "o.sqlite"
    jl = tmp_path / "rows.jsonl"
    rec = {
        "ts_utc": "2026-03-02T01:00:00Z",
        "book": "booka",
        "market_type": "h2h",
        "event_key": "e2",
        "participant_key": "HOME",
        "american_odds": 150,
    }
    jl.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        ["close-archive", "append", str(db), "--file", str(jl)],
    )
    main()
    with connect(db) as conn:
        assert conn.execute("select count(*) c from odds_snapshot").fetchone()["c"] == 1

def test_revised_quote_and_market_isolation(tmp_path):
    db = tmp_path / "odds.sqlite"
    init_db(db)
    with connect(db) as conn:
        for market, odds in [("h2h", -110), ("h2h", -112), ("spreads", 101)]:
            insert_row(conn, ts_utc="2026-01-01T12:00:00Z", book="book", market_type=market,
                       event_key="event", participant_key="home", american_odds=odds,
                       line=None, implied_prob=None, raw=None)
        params = {"event_key": "event", "participant_key": "home", "before_utc": "2026-01-01T12:00:00Z"}
        with pytest.raises(ValueError, match="--market"):
            latest_per_book(conn, **params)
        assert [q.american_odds for q in latest_per_book(conn, market_type="h2h", **params)] == [-112]
        assert latest_per_book(conn, market_type="other", **params) == []


def test_changed_point_line_omits_verdict(tmp_path, capsys):
    db = tmp_path / "odds.sqlite"
    init_db(db)
    with connect(db) as conn:
        for hour, line in [("12", 2.5), ("13", 3.5)]:
            insert_row(conn, ts_utc=f"2026-01-01T{hour}:00:00Z", book="book", market_type="spreads",
                       event_key="e", participant_key="p", american_odds=-110, line=line,
                       implied_prob=None, raw=None)
    cmd_beat_close(argparse.Namespace(db=db, event="e", participant="p", market=None,
                                      open_before="2026-01-01T12:00:00Z",
                                      close_before="2026-01-01T13:00:00Z"))
    assert capsys.readouterr().out.splitlines()[1].endswith(",")
