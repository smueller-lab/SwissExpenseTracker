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

---

## Entry Points

| File | Purpose |
|------|---------|
| `pipeline_ingestion/pipeline.py` | `run_ingestion()` — runs all four stages in sequence |
| `pipeline_ingestion/stages/stage_01_landing.py` | `run_landing()` |
| `pipeline_ingestion/stages/stage_02_raw.py` | `run_raw()` |
| `pipeline_ingestion/stages/stage_03_refined.py` | `run_refined()` |
| `pipeline_ingestion/stages/stage_04_postprocess.py` | `run_postprocess()` |

---

## Supported Data Sources

Defined as `SourceType` (`StrEnum`) in `data_models/source_type.py`.

| SourceType | Bank / Export format |
|------------|---------------------|
| `ZKB_DEBIT` | ZKB current account CSV |
| `VISECA` | Viseca credit card CSV |
| `REVOLUT` | Revolut account statement CSV |
| `SWISSQUOTE` | Swissquote brokerage export (reserved) |

The mapping from `SourceType` to Pydantic validation model is in
`data_models/data_sources.py → SOURCE_MODEL_MAP`.

Each source also has a corresponding **Adapter** (`adapters/adapters.py`) that
converts the source-specific validated model to the common `UnifiedTransaction`.

---

## Stage 1 — Landing

**File:** `stages/stage_01_landing.py`

1. Scans each source's sub-folder inside the landing zone (`lnd/`).
2. Computes an **MD5 hash** for each file and checks against `ingested_files`.
   A file is skipped if `(filename, hash, source_type)` already exists.
3. New files are inserted into `ingested_files` and each CSV/Excel row is
   stored as raw JSON in `transactions_lnd` (`processed = 0`).

**Duplicate detection:** file-level, by hash. Re-importing the same file is a no-op.
Delimiter detection is automatic (comma / semicolon).

---

## Stage 2 — Raw

**File:** `stages/stage_02_raw.py`

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

**File:** `stages/stage_03_refined.py`

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

**File:** `stages/stage_04_postprocess.py`

Cleans up known data quality issues in `transactions_rfn`:

1. **Credit-card double-booking removal** (`_clean_credit_card_payments`):
   ZKB debit account shows a "Viseca Payment" debit, and the Viseca CSV shows
   the matching settlement credit. Both represent the same money movement.
   Matched by amount and deleted to avoid double-counting.

2. **Viseca fee text fill** (`_fill_viseca_credit_card_fee_text`):
   Viseca rows with blank `booking_text` / `merchant_normalized` for credit
   card annual fees get a canonical label injected.

---

## Database Tables

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
| `LANDING_ZONE_DIR` | `DIR_BOX / "lnd"` (from `user_config.py`) |

`user_config.py` (not in version control) defines `DIR_BOX` — the root of the
local data folder where bank exports are dropped.
