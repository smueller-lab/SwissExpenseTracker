# 06 — Database & SQL Layer

How the app talks to SQLite. All SQL lives in `.sql` files under
`src/swiss_exp_tracker/db/queries/` and is loaded into callable Python functions
with **aiosql**. No raw SQL strings are written inline in pipeline, loader, or
callback code.

---

## The two databases

| File | Built by | Holds |
|------|----------|-------|
| `database/transactions.db` | ingestion + agentic + dashboard pipelines | transactions, groceries, merchant metadata, `api_usage`, all `dash_*` tables |
| `database/positions.db` | positions sub-pipeline | Swissquote position snapshots only |

Paths come from `pipeline_ingestion/config.py` (`INGESTION_DB_PATH`,
`POSITIONS_DB_PATH`), both resolved relative to `<project_root>/database/`.
Table-by-table schemas are documented in
[`02-ingestion-pipeline.md`](02-ingestion-pipeline.md) (ingestion, positions,
groceries), [`01-agentic-pipeline.md`](01-agentic-pipeline.md) (merchant/grocery
metadata, `api_usage`), and [`04-pipeline-dash.md`](04-pipeline-dash.md) (`dash_*`).

---

## aiosql query modules

**File:** `src/swiss_exp_tracker/db/sql.py`

Each `.sql` file is loaded once at import into a module-level object whose
attributes are the named queries inside it:

```python
import aiosql
transactions = aiosql.from_path(_QUERIES_DIR / "transactions.sql", "sqlite3")
groceries    = aiosql.from_path(_QUERIES_DIR / "groceries.sql",    "sqlite3")
positions    = aiosql.from_path(_QUERIES_DIR / "positions.sql",    "sqlite3")
agentic      = aiosql.from_path(_QUERIES_DIR / "agentic.sql",      "sqlite3")
```

| Module | Query file | Count | Used by |
|--------|-----------|-------|---------|
| `transactions` | `queries/transactions.sql` | ~67 | transaction stages, `dash_*` builders, `loader.py` |
| `groceries` | `queries/groceries.sql` | ~28 | grocery stages, grocery enrichment |
| `positions` | `queries/positions.sql` | ~25 | positions stages, `loader_positions.py` |
| `agentic` | `queries/agentic.sql` | ~33 | merchant enrichment, post-clean |

Import them as `from swiss_exp_tracker.db.sql import transactions` (etc.).

---

## Calling convention

Every query name in a `.sql` file is decorated with an **aiosql operator suffix**
that determines what the generated Python function does:

| Suffix | Meaning | Example name |
|--------|---------|--------------|
| (none) | SELECT → list of rows | `get_unprocessed_groceries_raw_rows` |
| `$` | SELECT → single scalar / row | `check_duplicate_groceries_rfn$` |
| `!` | INSERT/UPDATE/DELETE → rowcount | `mark_groceries_raw_processed!` |
| `*!` | executemany (batch write) | `insert_groceries_lnd*!` |
| `#` | DDL — `CREATE TABLE`/`INDEX`/`VIEW` | `create_groceries_rfn_table#` |

Functions take the connection as the first positional arg and named bind params
as keywords:

```python
from swiss_exp_tracker.db.sql import groceries, transactions

# write (suffix !): connection first, then named params
groceries.mark_groceries_raw_processed(db, raw_id=raw_id)

# scalar read (suffix $)
file_id = groceries.get_file_id_for_groceries_raw_row(db, raw_id=raw_id)

# DDL (suffix #)
transactions.create_transactions_rfn_table(db)
```

For pandas reads, pass the **raw SQL** via the `.sql` attribute instead of calling
the function (`pd.read_sql` wants a query string, not aiosql's row handling):

```python
self.pdf_Master = pd.read_sql(transactions.get_transactions_use.sql, con)
```

The same `.sql` attribute is used with `db.execute(query.sql)` when an inline
`ALTER TABLE` is needed (see `create_all_tables` migrations below).

---

## Connection helpers

Connections are obtained from context managers, never opened ad hoc in stage code:

| Helper | DB | File |
|--------|----|----- |
| `get_connection()` | `transactions.db` | `pipeline_ingestion/db.py` |
| `get_positions_connection()` | `positions.db` | `pipeline_ingestion/db_positions.py` |

Both `yield` a `sqlite3.Connection`, enable `PRAGMA foreign_keys = ON`, commit on
success, and close in a `finally`. The dashboard loaders open their own short-lived
read connections with `sqlite3.connect(...)` directly for `pd.read_sql`.

---

## Table creation & migrations

DDL is also stored as aiosql `#` queries and invoked from three setup functions,
all called at the top of the ingestion run (`pipeline_ingestion/pipeline.py`):

| Function | DB | File |
|----------|----|----- |
| `create_all_tables()` | `transactions.db` | `db.py` |
| `create_grocery_tables()` | `transactions.db` | `db_groceries.py` |
| `create_positions_tables()` | `positions.db` | `db_positions.py` |

All DDL uses `CREATE TABLE IF NOT EXISTS`, so these are safe to call on every run.

**Lightweight column migrations** live in `create_all_tables()`: it reads the
current `transactions_rfn` columns (`get_rfn_column_names`) and, if a column is
absent, runs an `ALTER TABLE ADD COLUMN` and backfills:

- `is_person` — added via `alter_rfn_add_is_person`.
- `balance_chf` — added via `alter_rfn_add_balance_chf`, then `_backfill_balance_chf_rfn`
  parses `"Balance CHF"` out of the stored raw JSON for ZKB rows.

(The dashboard pipeline has its own `_ensure_balance_chf` migration for
`transactions_use` — see [`04-pipeline-dash.md`](04-pipeline-dash.md).)

---

## Rules (enforced by project conventions)

- **No inline SQL** in layout, callback, loader, or stage files — add a named query
  to the relevant `.sql` file and call it. See `.claude/rules/data-models.md`.
- DB access belongs in `loader.py` / `loader_positions.py` (app side) and the
  stage / `db*.py` modules (pipeline side).
- When adding a table or query: add the `-- name: ...<suffix>` block to the right
  `.sql` file; it becomes a callable attribute on the matching `db.sql` module
  automatically — no Python change needed in `sql.py`.
