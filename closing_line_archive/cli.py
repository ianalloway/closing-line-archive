"""CLI for closing-line-archive."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from closing_line_archive.beat_close import implied_from_american, latest_per_book, price_rank
from closing_line_archive.db import connect, init_db, insert_row, parse_line_obj


def cmd_init(args: argparse.Namespace) -> None:
    init_db(Path(args.db))
    print(f"initialized {args.db}", file=sys.stderr)


def _append_stream(conn, fh) -> int:
    n = 0
    for line in fh:
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if not isinstance(obj, dict):
            raise ValueError("each line must be a JSON object")
        p = parse_line_obj(obj)
        insert_row(
            conn,
            ts_utc=p["ts_utc"],
            book=p["book"],
            market_type=p["market_type"],
            event_key=p["event_key"],
            participant_key=p["participant_key"],
            american_odds=p["american_odds"],
            line=p["line"],
            implied_prob=p["implied_prob"],
            raw=p["raw"],
        )
        n += 1
    return n


def cmd_append(args: argparse.Namespace) -> None:
    path = Path(args.db)
    init_db(path)
    with connect(path) as conn:
        if args.file:
            with open(args.file, encoding="utf-8") as fh:
                n = _append_stream(conn, fh)
        else:
            n = _append_stream(conn, sys.stdin)
        conn.commit()
    print(f"appended {n} row(s)", file=sys.stderr)


def cmd_export_csv(args: argparse.Namespace) -> None:
    path = Path(args.db)
    with connect(path) as conn:
        cur = conn.execute(
            "SELECT ts_utc, book, market_type, event_key, participant_key, "
            "american_odds, line, implied_prob FROM odds_snapshot ORDER BY ts_utc, book"
        )
        w = csv.writer(sys.stdout)
        w.writerow([d[0] for d in cur.description])
        for row in cur:
            w.writerow(list(row))


def cmd_beat_close(args: argparse.Namespace) -> None:
    path = Path(args.db)
    with connect(path) as conn:
        open_q = latest_per_book(
            conn,
            event_key=args.event,
            participant_key=args.participant,
            before_utc=args.open_before,
        )
        close_q = latest_per_book(
            conn,
            event_key=args.event,
            participant_key=args.participant,
            before_utc=args.close_before,
        )
    by_book_open = {q.book: q for q in open_q}
    by_book_close = {q.book: q for q in close_q}
    books = sorted(set(by_book_open) | set(by_book_close))
    w = csv.writer(sys.stdout)
    w.writerow(
        [
            "book",
            "open_ts",
            "open_american",
            "open_implied",
            "close_ts",
            "close_american",
            "close_implied",
            "beat_close",
        ]
    )
    for book in books:
        o = by_book_open.get(book)
        c = by_book_close.get(book)
        o_am = o.american_odds if o else None
        c_am = c.american_odds if c else None
        o_imp = implied_from_american(o_am) if o_am is not None else None
        c_imp = implied_from_american(c_am) if c_am is not None else None
        beat = ""
        if o_am is not None and c_am is not None:
            beat = "yes" if price_rank(o_am) >= price_rank(c_am) else "no"
        w.writerow(
            [
                book,
                o.ts_utc if o else "",
                o_am if o_am is not None else "",
                f"{o_imp:.4f}" if o_imp is not None else "",
                c.ts_utc if c else "",
                c_am if c_am is not None else "",
                f"{c_imp:.4f}" if c_imp is not None else "",
                beat,
            ]
        )


def main() -> None:
    p = argparse.ArgumentParser(description="Append and query odds snapshots (SQLite).")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Create database schema")
    p_init.add_argument("db", help="Path to SQLite file")
    p_init.set_defaults(func=cmd_init)

    p_app = sub.add_parser("append", help="Append JSONL rows (stdin or --file)")
    p_app.add_argument("db")
    p_app.add_argument("--file", type=str, default="")
    p_app.set_defaults(func=cmd_append)

    p_exp = sub.add_parser("export-csv", help="Dump all rows as CSV")
    p_exp.add_argument("db")
    p_exp.set_defaults(func=cmd_export_csv)

    p_bc = sub.add_parser(
        "beat-close",
        help="Per book: latest quote at/before open vs close cutoffs (CSV to stdout)",
    )
    p_bc.add_argument("db")
    p_bc.add_argument("--event", required=True)
    p_bc.add_argument("--participant", required=True)
    p_bc.add_argument("--open-before", required=True, help="ISO8601 UTC (inclusive ceiling for ts_utc)")
    p_bc.add_argument("--close-before", required=True)
    p_bc.set_defaults(func=cmd_beat_close)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
