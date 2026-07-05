Guide the user through adding a new bank or card CSV/XLSX export as a transaction source — from raw file to dashboard.

Adding a source is now **declarative**: a source is described by a `SourceProfile`
(a YAML block) plus a sample file. There is no per-source Pydantic model, no
adapter class, and no map registration. A single generic parser + generic
`to_unified` adapter, driven by the profile, handle every source. Hand-written
code is needed only for a genuinely *algorithmic* quirk (e.g. Revolut exchange
pairing) — ordinary banks need none.

> Prerequisite: this workflow assumes the `SourceProfile` system from
> `plan-source-profile.md` is implemented (`data_models/source_profile.py`,
> `data_models/profile_loader.py`, `adapters/generic_adapter.py`, profile-driven
> `stage_01_landing.py` / `stage_03_refined.py`). If those do not exist yet, run
> `/team-build plan-source-profile.md` first.

## Before writing any code

Ask the user to share:
1. The first 5–8 rows of the CSV/XLSX (header + a few data rows, **raw**, so any
   metadata preamble and the exact header encoding are visible).
2. What each column means: which is the date, amount, merchant/description,
   currency, running balance (if any), reference/transaction id.
3. Sign / direction convention: separate debit & credit columns, or a single
   signed amount column? If signed, is a **positive** value an expense or income?
4. Date format(s) used (e.g. `DD.MM.YYYY`, ISO `YYYY-MM-DD`, ISO with time).
5. Are the headers non-English (French/German/Italian) and/or is there a metadata
   preamble above the real header row? (Determines `header_signature` /
   `column_aliases`.)

Do not proceed until these are answered.

## Step 1 — Register the source type

File: `pipeline_ingestion/data_models/source_type.py`

Add a new `SourceType` enum value in `SCREAMING_SNAKE_CASE`:
```python
NEON = "NEON"
```
This is the only Python edit for an ordinary source.

## Step 2 — Add the SourceProfile (the main step)

File: `pipeline_ingestion/data_models/source_profiles.yaml`

Add one block keyed by the enum value. All column references use the **canonical**
column name (the right-hand side of `column_aliases`; for ASCII headers that is
just the raw header). Annotated template:

```yaml
NEON:
  detect_required_columns: ["Date", "Amount", "Description"]   # forward-looking; not yet wired to auto-detect
  # --- header handling (omit both for plain ASCII headers with no preamble) ---
  header_signature: null            # column whose row marks the start of the table (skips a metadata preamble)
  column_aliases: {}                # {raw header -> canonical name}, matched accent/mojibake-insensitive
  # --- field mapping ---
  date_columns: ["Date", "Value date"]   # coalesced in order; first present, parseable value wins
  date_formats: ["%d.%m.%Y", "iso"]      # strptime patterns; literal "iso" -> datetime.fromisoformat
  reference_columns: ["Transaction id"]  # coalesced; empty list -> NOID-<uuid> fallback
  booking_text_columns: ["Description"]  # coalesced; first non-empty
  amount:
    mode: debit_credit              # "debit_credit" OR "signed"
    # -- debit_credit: direction comes from WHICH column is populated; sign inside is ignored (abs) --
    debit_column: "Debit"
    credit_column: "Credit"
    fallback_amount_column: null    # optional: when debit & credit both empty -> abs() as EXPENSE
    # -- signed (single column): direction comes from the SIGN --
    # amount_column: "Amount"
    # expense_sign: negative        # "negative" (e.g. Revolut) or "positive" (e.g. Viseca)
  currency:
    mode: column                    # "column" (read from a column) OR "fixed"
    column: "Currency"              # required when mode: column
    default: CHF                    # fallback / the value when mode: fixed
  balance_column: null              # optional: running account balance column (debit accounts only)
```

Key decisions:
- **Two columns (Debit/Credit) → `mode: debit_credit`.** Sign inside the column
  is irrelevant — `abs()` is taken and direction is decided by which column has a
  value. Use `fallback_amount_column` for sources that occasionally put the value
  in a third "amount" column (e.g. foreign-currency legs).
- **One signed column → `mode: signed`** with `amount_column` + `expense_sign`.
- **Foreign headers / preamble:** set `header_signature` to the column that marks
  where the table starts, and fill `column_aliases` with `{raw → canonical}`. The
  shared canonicalizer matches accent- and mojibake-insensitively, so a clean
  `"Débit"` and a cp1252-mangled `"D�bit"` both resolve to the canonical name.

## Step 3 — Add the sample file

Add a representative export to `tests/test_data/<source_name>_test.csv` (or
`.xlsx`). Keep it small (a few rows) but include the **real header** — preamble
and original encoding intact for foreign-header sources. This file is both the
inference input and the regression fixture; the generic parametrized test picks
it up automatically.

## Step 4 — Source-specific logic (only if truly needed)

File: `pipeline_ingestion/stages/transactions/source_hooks.py`

The generic parser + adapter handle field mapping completely. Add a
`SourceType`-keyed hook **only** for genuinely algorithmic quirks that cannot be
expressed as data:
- Paired-row filtering / reconstruction (e.g. Revolut exchange legs).
- Multi-row date/reference inheritance (e.g. ZKB "Debit eBanking" parent/detail).
- A non-standard CHF conversion path.

If none apply, state explicitly that no hook is needed.

## Step 5 — Landing zone folder

The landing stage auto-creates `LANDING_ZONE_DIR / source_type.value.lower()`
from the enum value. No code change. Tell the user where to drop files:
```
lnd/neon/   ← drop CSV/XLSX exports here before running the pipeline
```

## Step 6 — Cross-source dedup (only if needed)

File: `pipeline_ingestion/stages/transactions/stage_04_postprocess.py`

Add logic only if the new source creates transactions that duplicate entries from
another source (e.g. a credit-card payment appearing on both the card statement
and the linked bank account). Otherwise state explicitly that no dedup is needed.

## Step 7 — Tests

The single parametrized invariant test (`tests/test_pipeline_transactions/
test_generic_adapter.py`) automatically covers the new source once its profile +
sample file exist — it asserts `isinstance(u, UnifiedTransaction)`,
`u.amount >= 0`, `u.transaction_type in TransactionType`, non-empty
`source_file`, and that present dates parse.

Hand off to the `tester` agent only for **source-specific behavior**: any
`source_hooks.py` quirk added in Step 4, plus an edge case for the new date
format / sign convention if unusual. No new per-source test module is required.

## Step 8 — Run the ingestion pipeline and verify

User drops a sample file in `lnd/neon/` and runs:
```
python -m swiss_exp_tracker.pipeline_ingestion.pipeline
```
Check that `ingested_files`, `transactions_lnd`, `transactions_raw`, and
`transactions_rfn` show the expected row counts, and that
`enrichment_status = "pending"` rows exist in `transactions_rfn`.

## Step 8b — Run the agentic pipeline

Enriches `transactions_rfn` with category labels; must run before dashboard work.
```
python -m swiss_exp_tracker.pipeline_agentic.pipeline
```
Verify `enrichment_status` moved from `"pending"` to `"done"`. Investigate any
`"pending"` / `"failed"` rows before proceeding.

## Step 8c — Run the dashboard pipeline

Rebuild the `dash_*` tables so the dashboard picks up the new transactions:
```
python -m swiss_exp_tracker.pipeline_dash.pipeline
```
Verify the affected `dash_*` tables reflect the new source. The dashboard reads
exclusively from these tables — skipping this step means the data will not appear.

## Step 9 — Dashboard integration

- If the source introduces new merchant names or categories needing specific
  colors, add them to `VIS.vk_*_col` in `app/config.py`.
- If the source uses a currency not yet in the `Currency` enum
  (`data_models/transaction.py`), add it there first.
- The home page loader reads all sources from `transactions_rfn` without source
  filtering — no change unless the source must be excluded.
- If a dedicated dashboard page is warranted, run `/new-page` afterwards.

## After all steps

```
poetry run python -m ruff check <modified files>
poetry run python -m black --check <modified files>
poetry run mypy --config-file pyproject.toml src
```

For an ordinary new source the full diff is: **one enum line + one YAML block +
one sample file.** No model, no adapter, no map edits, no stage edits.
