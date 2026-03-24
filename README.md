# closing-line-archive

[![CI](https://github.com/ianalloway/closing-line-archive/actions/workflows/ci.yml/badge.svg)](https://github.com/ianalloway/closing-line-archive/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Problem:** CLV workflows need a **durable, queryable** history of book prices — not just the latest API pull.

**Solution:** **`close-archive`** writes normalized odds snapshots to **SQLite** (one row per quote). Append **JSONL** from cron or your **line-shop-cli** adapter. **`beat-close`** compares, per book, the latest quote at **open** vs **close** cutoffs and marks whether your **open** price was **no worse** than the **close** (American-odds ordering: −105 beats −110, +150 beats +140).

## JSONL row shape

```json
{"ts_utc":"2026-03-01T19:30:00Z","book":"draftkings","market_type":"spreads","event_key":"nba:20260301:LAL@BOS","participant_key":"LAL","american_odds":-108,"line":7.5}
```

Optional: `implied_prob`, any extra fields are preserved in `raw_json`.

## Commands

```bash
pip install -e .
close-archive init odds.sqlite
close-archive append odds.sqlite --file snapshots.jsonl
# or:  your_generator | close-archive append odds.sqlite

close-archive export-csv odds.sqlite > dump.csv

close-archive beat-close odds.sqlite \
  --event "nba:20260301:LAL@BOS" \
  --participant LAL \
  --open-before "2026-03-01T18:00:00Z" \
  --close-before "2026-03-01T23:00:00Z"
```

Use **ISO8601 UTC** strings for timestamps (lexicographic compare matches time order).

## Pairing with the rest of the stack

- Emit JSONL from **[line-shop-cli](https://github.com/ianalloway/line-shop-cli)** with a thin mapper.
- Feed eval summaries into **[nba-clv-dashboard](https://github.com/ianalloway/nba-clv-dashboard)** / **backtest-report-gen**.

## Non-goals

- **No** live book API — ingest only.
- **Parquet export** — optional extra (`pip install '.[parquet]'`) not implemented in v0.1; use `export-csv` or DuckDB on SQLite.

## License

MIT
