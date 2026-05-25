# 02 — Ingestion Pipeline

## Overview

The ingestion pipeline reads bank CSV/Excel exports from the landing zone,
validates and normalises them through four sequential stages, and writes the
results into SQLite. Each stage is idempotent — re-running it only processes
rows not yet marked as processed.

```
Landing Zone (filesystem)
  lnd/*.csv  /  lnd/*.xlsx
        │
        ▼
  Stage 1 — Landing
  detect new files by MD5 hash → parse rows → store raw JSON
  ──────────────────────────────────────────────────────────
  ingested_files   (file registry)
  transactions_lnd (raw JSON rows, processed=0)
        │
        ▼
  Stage 2 — Raw
  validate each lnd row against source-specific Pydantic model
  ──────────────────────────────────────────────────────────
  transactions_raw (validated JSON, processed=0)
        │
        ▼
  Stage 3 — Refined
  unify fields across sources, extract merchant, detect persons
  ──────────────────────────────────────────────────────────
  transactions_rfn (canonical rows, enrichment_status='pending')
        │
        ▼
  Stage 4 — Postprocess
  remove credit-card double-bookings, fill missing texts
```

The positions pipeline runs as Stage 5 inside `run_ingestion()` and writes to a
separate `positions.db`. See [Swissquote Positions Pipeline](#swissquote-positions-pipeline).

The grocery pipeline runs as Stage 6 inside `run_ingestion()` and writes to `transactions.db`.
See [Migros Grocery Pipeline](#migros-grocery-pipeline).

---

## Entry Points

| File | Purpose |
|------|---------|
| `pipeline_ingestion/pipeline.py` | `run_ingestion()` — runs all stages in sequence |
| `pipeline_ingestion/stages/transactions/stage_01_landing.py` | `run_landing()` |
| `pipeline_ingestion/stages/transactions/stage_02_raw.py` | `run_raw()` |
| `pipeline_ingestion/stages/transactions/stage_03_refined.py` | `run_refined()` |
| `pipeline_ingestion/stages/transactions/stage_04_postprocess.py` | `run_postprocess()` |
| `pipeline_ingestion/stages/positions/stage_01_landing.py` | `run_positions_landing()` |
| `pipeline_ingestion/stages/positions/stage_02_raw.py` | `run_positions_raw()` |
| `pipeline_ingestion/stages/positions/stage_03_rfn.py` | `run_positions_rfn()` |
| `pipeline_ingestion/stages/positions/stage_04_use.py` | `run_positions_use()` |
| `pipeline_ingestion/stages/groceries/stage_01_landing.py` | `run_groceries_landing()` |
| `pipeline_ingestion/stages/groceries/stage_02_raw.py` | `run_groceries_raw()` |
| `pipeline_ingestion/stages/groceries/stage_03_rfn.py` | `run_groceries_rfn()` |
| `pipeline_ingestion/stages/groceries/stage_04_use.py` | `run_groceries_use()` |

---

## Supported Data Sources

Defined as `SourceType` (`StrEnum`) in `data_models/source_type.py`.

| SourceType | Bank / Export format |
|------------|---------------------|
| `ZKB_DEBIT` | ZKB current account CSV |
| `VISECA` | Viseca credit card CSV |
| `REVOLUT` | Revolut account statement CSV |
| `SWISSQUOTE` | Swissquote positions XLS snapshot |
| `MIGROS_GROCERY` | Migros receipt CSV export |

The mapping from `SourceType` to Pydantic validation model is in
`data_models/data_sources.py → SOURCE_MODEL_MAP` (transactions only).

Each transaction source also has a corresponding **Adapter** (`adapters/adapters.py`) that
converts the source-specific validated model to the common `UnifiedTransaction`.

---

## Stage 1 — Landing

**File:** `stages/transactions/stage_01_landing.py`

1. Scans each source's sub-folder inside the landing zone (`lnd/`).
2. Computes an **MD5 hash** for each file and checks against `ingested_files`.
   A file is skipped if `(filename, hash, source_type)` already exists.
3. New files are inserted into `ingested_files` and each CSV/Excel row is
   stored as raw JSON in `transactions_lnd` (`processed = 0`).

**Duplicate detection:** file-level, by hash. Re-importing the same file is a no-op.
Delimiter detection is automatic (comma / semicolon).

---

## Stage 2 — Raw

**File:** `stages/transactions/stage_02_raw.py`

1. Loads all `transactions_lnd` rows where `processed = 0`.
2. Validates each row's JSON against the source-specific Pydantic model
   (`SOURCE_MODEL_MAP`). This catches type errors, missing required fields,
   and invalid enum values.
3. Writes the re-serialised (validated) JSON into `transactions_raw`
   (`processed = 0`) and marks the lnd row as `processed = 1`.

**Purpose:** enforce a typed contract at the source boundary before any
transformation logic runs.

---

## Stage 3 — Refined

**File:** `stages/transactions/stage_03_refined.py`

The heaviest stage. For each unprocessed `transactions_raw` row:

1. Deserialises JSON back into the source model.
2. Passes the model through the corresponding **Adapter** → `UnifiedTransaction`
   (canonical fields: `date`, `amount`, `currency`, `transaction_type`,
   `booking_text`, `zkb_reference`).
3. Runs **merchant normalisation** (`_normalize_merchant`):
   - Lowercases, strips umlauts
   - Applies `MERCHANT_COMPOUND_BRANDS` (longest-match patterns, e.g. "migros m express")
   - Applies `MERCHANT_BRANDS` (simple brand list)
   - Falls back to stripping digits, punctuation, and legal suffixes (AG, GmbH, SA)
4. Detects **person transactions** (`_is_person_transaction`):
   - Looks for phone-like digit sequences (≥ 7 digits) in TWINT transfers
5. Writes to `transactions_rfn` with `enrichment_status = 'pending'`.

**Merchant brand lists** live in `pipeline_agentic/data_models/merchant.py`:
- `MERCHANT_BRANDS` — simple substring match
- `MERCHANT_COMPOUND_BRANDS` — ordered list of `(pattern, canonical)` pairs
  (checked first, longest-match wins)

---

## Stage 4 — Postprocess

**File:** `stages/transactions/stage_04_postprocess.py`

Cleans up known data quality issues in `transactions_rfn`:

1. **Credit-card double-booking removal** (`_clean_credit_card_payments`):
   ZKB debit account shows a "Viseca Payment" debit, and the Viseca CSV shows
   the matching settlement credit. Both represent the same money movement.
   Matched by amount and deleted to avoid double-counting.

2. **Viseca fee text fill** (`_fill_viseca_credit_card_fee_text`):
   Viseca rows with blank `booking_text` / `merchant_normalized` for credit
   card annual fees get a canonical label injected.

---

## Database Tables — `transactions.db`

All tables live in `database/transactions.db`. Created by `db.py → create_all_tables()`.

### `ingested_files`
Registry of all processed source files.

| Column | Notes |
|--------|-------|
| `filename` | Original filename |
| `file_hash` | MD5 hash — used for deduplication |
| `source_type` | `SourceType` value |
| `ingested_at` | ISO timestamp |
| `record_count` | Number of rows in the file |
| `status` | Processing status |

**Unique constraint:** `(filename, file_hash, source_type)` — prevents re-import.

### `transactions_lnd`
Raw JSON dump, one row per CSV/Excel row.

| Column | Notes |
|--------|-------|
| `file_id` | FK → `ingested_files.id` |
| `source_type` | `SourceType` value |
| `raw_json` | Original row as JSON string |
| `processed` | 0 = pending, 1 = promoted to raw |

### `transactions_raw`
Pydantic-validated JSON, one row per landing row.

| Column | Notes |
|--------|-------|
| `landing_id` | FK → `transactions_lnd.id` |
| `source_type` | `SourceType` value |
| `raw_json` | Validated and re-serialised JSON |
| `source_file` | Original filename |
| `processed` | 0 = pending, 1 = promoted to refined |

### `transactions_rfn`
Canonical, analysis-ready transaction rows. Input to the agentic pipeline.

| Column | Notes |
|--------|-------|
| `raw_id` | FK → `transactions_raw.id` |
| `source_type` | `SourceType` value |
| `date` | ISO date string |
| `amount` | Always positive; sign meaning comes from `transaction_type` |
| `transaction_type` | `EXPENSE` or `INCOME` |
| `booking_text` | Original text from bank |
| `merchant_normalized` | Cleaned merchant name (see Stage 3) |
| `is_person` | 1 if detected as person-to-person (TWINT + phone number) |
| `currency` | Original transaction currency |
| `reference` | ZKB reference number — used to JOIN with merchant metadata |
| `enrichment_status` | `pending` → `enriched` (set by agentic pipeline) |

---

## Data Models

All models are Pydantic `BaseModel` subclasses in `data_models/`.

| Model | File | Purpose |
|-------|------|---------|
| `ZKBTransaction` | `transaction.py` | Raw ZKB CSV row |
| `VisecaTransaction` | `transaction.py` | Raw Viseca CSV row |
| `RevolutTransaction` | `transaction.py` | Raw Revolut CSV row |
| `UnifiedTransaction` | `transaction.py` | Common format across all sources |
| `LandingRow` | `tables.py` | DB read model for `transactions_lnd` |
| `RawRow` | `tables.py` | DB read model for `transactions_raw` |

Adapters (`adapters/adapters.py`) implement `BaseAdapter[S].to_unified()` which converts
a source-specific model `S` into a `UnifiedTransaction`.

---

## File Tracker

`file_tracker.py` handles landing-zone scanning and deduplication:

- `get_new_files(folder, source_type)` — returns files not yet in `ingested_files`
  (matched by `(filename, MD5)`). Supports `.csv`, `.xlsx`, `.xls`.
- `mark_file_processed(file_id)` — updates the status in `ingested_files`.

---

## Configuration

`config.py` exposes:

| Constant | Value |
|----------|-------|
| `INGESTION_DB_PATH` | `<project_root>/database/transactions.db` |
| `POSITIONS_DB_PATH` | `<project_root>/database/positions.db` |
| `LANDING_ZONE_DIR` | `DIR_BOX / "lnd"` (from `user_config.py`) |

`user_config.py` (not in version control) defines `DIR_BOX` — the root of the
local data folder where bank exports are dropped.

---

---

# Swissquote Positions Pipeline

Positions are snapshot-based (one file = one day's holdings), not event-based transactions.
They live in a separate `database/positions.db` — no shared schema with `transactions.db`,
no merchant enrichment, no agentic pipeline.

`run_positions_pipeline()` in `pipeline.py` is called from `run_ingestion()` as Stage 5,
after all transaction stages complete.

---

## Input Format

Swissquote exports `.xls` files named `Positions_<account_id>_<DDMMYYYY>_<HH>_<MM>.xls`.
The snapshot date and account ID are parsed from the filename; no date column exists inside
the file.

The XLS is hierarchical, not flat. Four row types are present:

| Row type | Detection | Action |
|----------|-----------|--------|
| Header | row 0 | skip |
| Asset class | col[0] non-blank, col[1] blank | set current class |
| Position | col[0] blank, col[1] non-blank, col[2] numeric | ingest |
| Subtotal/Total | col[0] blank, col[1] non-blank, col[2] non-numeric | skip |

Position rows are detected by checking whether col[2] (quantity) is numeric — this works
regardless of the export language (English, German, etc.).

Column layout (0-indexed):

```
── in CCY (original currency, col[8]) ──────────────────────────────────
0: row-type marker (blank for position rows)
1: Symbol
2: Quantity
3: Unit cost           (purchase price per unit)
4: Total value         (current market value)
5: Daily change        (not ingested)
6: Daily chg. %        (not ingested)
7: Price               (current market price per unit)
8: CCY

── in CHF ───────────────────────────────────────────────────────────────
9: P&L Nominal CHF     (unrealised gain/loss)
10: P&L % CHF          (P&L as decimal fraction, e.g. 0.1002 = 10.02%)
11: Total value CHF
12: Positions %        (portfolio weight as decimal fraction, e.g. 0.031 = 3.1%)

── optional ─────────────────────────────────────────────────────────────
13: Name               (full asset name, present in newer exports)
```

Daily change columns (5, 6) are read but not stored — point-in-time noise with no
value for long-term progress tracking. Col[13] is read when present; older exports
without it store `NULL` in the `name` column.

---

## Files

### `pipeline_ingestion/config.py`
Adds `POSITIONS_DB_PATH = INGESTION_DB_DIR / "positions.db"`.

### `pipeline_ingestion/db_positions.py`
Connection context manager (`get_positions_connection`) and `create_positions_tables()`
for `positions.db`. Creates four tables plus indexes.

### `pipeline_ingestion/file_tracker_positions.py`
Mirrors `file_tracker.py` but targets `ingested_files_pos` in `positions.db`.
Files are deduped by MD5 hash — re-importing the same file is a no-op.

### `pipeline_ingestion/data_models/position.py`
Two Pydantic models:

**`SwissquotePositionRaw`** — used in landing and raw stages. `snapshot_date` is a plain
string (ISO date). NaN cells are converted to `None` via a `@field_validator`.
A `@model_validator` checks `quantity × price ≈ total_value` (tolerance 0.01 in CCY).

**`SwissquotePosition`** — used in the refined stage. Identical fields but `snapshot_date`
is typed as `datetime.date`. Carries the same validators.

Both models include an optional `name: str | None` field for the full asset name.

### `pipeline_ingestion/stages/positions/stage_01_landing.py`
Parses the XLS, emits one `SwissquotePositionRaw` per position row, inserts as JSON blobs
into `positions_lnd`, marks the file in `ingested_files_pos`.

### `pipeline_ingestion/stages/positions/stage_02_raw.py`
Re-validates landing blobs with `SwissquotePositionRaw`, writes to `positions_raw`.

### `pipeline_ingestion/stages/positions/stage_03_rfn.py`
Validates raw blobs with `SwissquotePosition`, inserts into `positions_rfn` with
`INSERT OR IGNORE` — the `UNIQUE(snapshot_date, symbol, account_id)` constraint handles
deduplication. Promotes file status to `'refined'` when all rows are processed.

### `pipeline_ingestion/stages/positions/stage_04_use.py`
Rebuilds `positions_use` from scratch on every run (`DELETE` + `INSERT … SELECT`),
renaming columns to shorter dashboard-friendly names.

---

## Database Tables — `positions.db`

### `ingested_files_pos`
Tracks each ingested XLS file by filename + MD5 hash. Same shape as `ingested_files` in
`transactions.db` but without the `source_type` column (always Swissquote).

### `positions_lnd`
Raw JSON blobs, one row per position per file. Mirrors `transactions_lnd`.

### `positions_raw`
Re-validated JSON blobs. Mirrors `transactions_raw`.

### `positions_rfn`
Canonical positions table. Deduped by `UNIQUE(snapshot_date, symbol, account_id)`.

```sql
snapshot_date       TEXT NOT NULL,   -- ISO: YYYY-MM-DD
account_id          TEXT NOT NULL,
asset_class         TEXT NOT NULL,   -- e.g. 'Shares', 'ETFs'
-- in CCY --
symbol              TEXT NOT NULL,
name                TEXT,            -- full asset name, NULL if not in export
quantity            REAL NOT NULL,
unit_cost           REAL,
price               REAL NOT NULL,
currency            TEXT NOT NULL,
total_value         REAL,
-- in CHF --
total_value_chf     REAL NOT NULL,
pnl_nominal_chf     REAL,
pnl_pct_chf         REAL,            -- decimal fraction, e.g. 0.1002 = 10.02%
position_weight_pct REAL,
-- metadata --
source_file         TEXT NOT NULL,
created_at          TEXT NOT NULL
```

### `positions_use`
Clean dashboard-ready table, rebuilt on every pipeline run. No metadata columns.

```sql
date        TEXT NOT NULL,   -- snapshot_date
account     TEXT NOT NULL,   -- account_id
asset_class TEXT NOT NULL,
symbol      TEXT NOT NULL,
name        TEXT,            -- full asset name, NULL if not in export
quantity    REAL NOT NULL,
unit_cost   REAL,            -- in CCY
price       REAL NOT NULL,   -- in CCY
currency    TEXT NOT NULL,
value       REAL,            -- total_value in CCY
value_chf   REAL NOT NULL,
pnl_chf     REAL,
pnl_pct     REAL,            -- decimal fraction, e.g. 0.1002 = 10.02%
weight_pct  REAL
```

---

## Pipeline Flow

```
lnd/swissquote/Positions_<account>_<DDMMYYYY>_<HH>_<MM>.xls
        │
        ▼  stage_01_landing  (stages/positions/)
positions_lnd  — JSON blobs, one per position row
        │
        ▼  stage_02_raw
positions_raw  — re-validated JSON blobs
        │
        ▼  stage_03_rfn
positions_rfn  — canonical, UNIQUE(snapshot_date, symbol, account_id)
        │
        ▼  stage_04_use
positions_use  — renamed columns, rebuilt from rfn on every run
```

Re-running the pipeline is safe: already-processed files are skipped at the file-tracker
level, and `INSERT OR IGNORE` prevents duplicate rows in `positions_rfn`.

---

## Adding New Snapshots

Drop a new `Positions_*.xls` into `lnd/swissquote/` and run the pipeline. The file is
picked up automatically on the next run.

---

---

# Migros Grocery Pipeline

Grocery receipts are imported from Migros CSV exports. Unlike transactions, each row
represents a single purchased article. The pipeline handles return rows (negative
quantities) via receipt-level netting before writing to the refined stage. LLM
categorization happens separately in the agentic pipeline (see `01-agentic-pipeline.md`).

`run_groceries_pipeline()` in `pipeline.py` is called from `run_ingestion()` as Stage 6.

---

## Input Format

Migros exports semicolon-delimited CSVs with German column headers:

| CSV Column | Field | Notes |
|------------|-------|-------|
| `Datum` | `date` | `DD.MM.YYYY` format |
| `Zeit` | `time` | `HH:MM:SS` format |
| `Filiale` | `location` | Store name |
| `Artikel` | `article` | Product name |
| `Menge` | `quantity` | Decimal = kg, integer = qty; negative = return |
| `Aktion` | `discount` | Discount amount in CHF |
| `Umsatz` | `price` | Price in CHF |
| `Kassennummer` | — | Ignored |
| `Transaktionsnummer` | — | Ignored |

**Bonus rows** (e.g. Cumulus point redemptions) have both `price = 0` and `discount = 0`.
They are flagged in landing but silently dropped in the refined stage.

---

## Article Normalisation

`normalize_article()` in `data_models/grocery.py` strips size/unit suffixes from article
names before they are stored in the vector store. This improves cache hit rates by
collapsing variants of the same product.

Three passes, applied in order:

1. **Count × size patterns** — `"6*53g"`, `"4x100g"`, `"2x500ml"` → removed
2. **Standalone unit suffixes** — `"330ml"`, `"50cl"`, `"100g"`, `"1kg"` → removed
3. **Trailing standalone integer** — `"Brot 750"` → `"Brot"`

Examples:
```
"Haribo Goldbären 100g"  →  "Haribo Goldbären"
"Aproz Gazeifiee 50cl"   →  "Aproz Gazeifiee"
"Eier Freiland 6*53g"    →  "Eier Freiland"
"Migros Wasser 1.5l"     →  "Migros Wasser"
"7UP"                    →  "7UP"   (preserved — no unit suffix)
```

---

## Return Row Netting

Migros receipts can contain negative-quantity rows when an item was scanned incorrectly.
The refined stage groups all rows by receipt `(date, time, location)` and then by article,
and sums their quantities and prices before writing to `groceries_rfn`.

| Net quantity | Outcome |
|---|---|
| > 0 | Written as a single row with summed price/discount |
| = 0 | Silently discarded (full return) |
| < 0 | Discarded with a `[WARN]` log (data anomaly) |

Netting is implemented in `net_receipt_rows()` (exposed at module level for testing).

---

## Files

### `pipeline_ingestion/db_groceries.py`
`create_grocery_tables()` — creates `groceries_lnd`, `groceries_raw`, `groceries_rfn`,
and all associated indexes (including a composite dedup index on `groceries_rfn`).

### `pipeline_ingestion/data_models/grocery.py`
Two data models and the normalisation function:

- **`GroceryUnit`** (`StrEnum`) — `kg` / `qty`
- **`GroceryItem`** — Pydantic model for a single CSV row. German column headers mapped
  via `Field(alias=...)`. Computed fields: `unit` (kg if quantity is non-integer),
  `is_bonus_row`, `is_return_row`. Date validator accepts both `DD.MM.YYYY` and ISO format.
- **`normalize_article(article)`** — see Article Normalisation above.

### `pipeline_ingestion/stages/groceries/stage_01_landing.py`
Reads CSV from `lnd/migros_grocery/`, validates each row into `GroceryItem`, inserts
into `groceries_lnd`. Bonus rows are flagged (`is_bonus_row = 1`). File deduplication
via MD5 hash in `ingested_files`.

### `pipeline_ingestion/stages/groceries/stage_02_raw.py`
Re-validates `groceries_lnd` rows, writes canonical JSON (English field names, ISO dates)
to `groceries_raw`. Marks landing rows processed.

### `pipeline_ingestion/stages/groceries/stage_03_rfn.py`
The heaviest stage:
1. Loads unprocessed `groceries_raw` rows, skips bonus rows (price = 0 and discount = 0).
2. Groups rows by receipt `(date, time, location)` then applies `net_receipt_rows()`.
3. Checks each netted item against `groceries_rfn` for duplicates on
   `(date, time, location, article, quantity)` — skips on match.
4. Inserts new rows with `enrichment_status = 'pending'`.
5. Marks all raw rows processed; advances file status to `'refined'` when complete.

### `pipeline_ingestion/stages/groceries/stage_04_use.py`
Joins `groceries_rfn` (where `enrichment_status = 'enriched'`) with the
`grocery_categorization_rfn` VIEW and writes the result to `groceries_use`.
Incremental — skips `rfn_id` values already present.

---

## Database Tables — `transactions.db`

### `groceries_lnd`
| Column | Notes |
|--------|-------|
| `file_id` | FK → `ingested_files.id` |
| `source_type` | Always `MIGROS_GROCERY` |
| `raw_json` | Original row as JSON (German field names) |
| `is_bonus_row` | 1 if price = 0 and discount = 0 |
| `processed` | 0 = pending, 1 = promoted to raw |

### `groceries_raw`
| Column | Notes |
|--------|-------|
| `landing_id` | FK → `groceries_lnd.id` |
| `source_type` | Always `MIGROS_GROCERY` |
| `raw_json` | Validated JSON (English field names, ISO dates) |
| `source_file` | Original filename |
| `processed` | 0 = pending, 1 = promoted to rfn |

### `groceries_rfn`
| Column | Notes |
|--------|-------|
| `raw_id` | FK → `groceries_raw.id` (NULL for multi-source netted rows) |
| `source_type` | Always `MIGROS_GROCERY` |
| `date` | ISO date string |
| `time` | `HH:MM:SS` string |
| `location` | Store name |
| `article` | Original product name |
| `article_normalized` | Normalised name (size/unit stripped) |
| `unit` | `kg` or `qty` |
| `quantity` | Net quantity after return netting |
| `price_chf` | Net price after return netting |
| `discount_chf` | Net discount after return netting |
| `enrichment_status` | `pending` → `enriched` (set by agentic pipeline) |

### `groceries_use`
Final analysis-ready table. JOIN of `groceries_rfn` + `grocery_categorization_rfn`.
Only rows with `enrichment_status = 'enriched'` are included. Incremental.

| Column | Notes |
|--------|-------|
| `rfn_id` | FK → `groceries_rfn.id` |
| `date`, `time`, `location` | From rfn |
| `article`, `unit`, `quantity` | From rfn |
| `price_chf`, `discount_chf` | From rfn |
| `category_main` | From agentic pipeline |
| `category_detail` | From agentic pipeline |

---

## Pipeline Flow

```
lnd/migros_grocery/*.csv
        │
        ▼  stage_01_landing
groceries_lnd  — raw JSON (German headers), bonus rows flagged
        │
        ▼  stage_02_raw
groceries_raw  — re-validated JSON (English headers, ISO dates)
        │
        ▼  stage_03_rfn
groceries_rfn  — netted, deduped, enrichment_status='pending'
        │
        ▼  [agentic pipeline — see 01-agentic-pipeline.md]
groceries_rfn  — enrichment_status='enriched'
        │
        ▼  stage_04_use
groceries_use  — rfn + categories, dashboard-ready
```

---

## Adding New Receipts

Drop a new Migros CSV export into `lnd/migros_grocery/` and run the pipeline.
The file is picked up automatically (MD5 dedup prevents re-import of the same file).
Return rows are netted within each receipt; cross-receipt duplicates are caught by
the `(date, time, location, article, quantity)` uniqueness check in stage 03.
