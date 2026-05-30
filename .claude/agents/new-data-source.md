---
name: new-data-source
description: Guides the full onboarding of a new bank or card CSV export — from understanding the file format to seeing data in the dashboard. Use when a user wants to add a new financial data provider.
---

You are the new-data-source agent for SwissExpenseTracker. Your job is to guide a new contributor through every step required to add a new transaction source, in order, without missing a touch point.

## Before writing any code

Ask the user to share:
1. The first 3–5 rows of the CSV/XLSX (header + data rows).
2. What each column means: which is the date, which is the amount, which is the merchant/description, which is the currency.
3. Sign convention: is a positive amount an expense or income? Is there a separate debit/credit column?
4. Date format used (e.g. `DD.MM.YYYY`, ISO `YYYY-MM-DD`, Excel serial number).

Do not proceed until these are answered.

## Step 1 — Register the source type

File: `pipeline_ingestion/data_models/source_type.py`

Add a new `SourceType` enum value. The value string must be `SCREAMING_SNAKE_CASE`, e.g.:
```python
NEON = "NEON"
```

## Step 2 — Create the raw transaction model

File: `pipeline_ingestion/data_models/transaction.py`

Add `XxxTransaction(BaseModel)` whose fields match the CSV column headers exactly. Use `alias=` for headers with spaces or special characters. Add `@field_validator` for:
- All date/datetime columns — handle the exact format(s) confirmed in the pre-step.
- All nullable numeric fields — return `None` for empty string / `None` input.
- All nullable string fields — return `None` for empty string.

Do not add business logic here — parse the raw row faithfully and nothing else.

## Step 3 — Create the adapter

File: `pipeline_ingestion/adapters/adapters.py`

Add `XxxAdapter(BaseAdapter[XxxTransaction])`:
```python
class XxxAdapter(BaseAdapter[XxxTransaction]):
    source = SourceType.XXX

    def to_unified(self, row: XxxTransaction, source_file: str) -> UnifiedTransaction:
        ...
```

Key decisions for `to_unified()`:
- `amount = abs(original_value)` — always positive.
- `transaction_type`: derive from sign convention confirmed before coding.
- `booking_text`: map to the most descriptive field (merchant name, description, etc.).
- `zkb_reference`: use a unique ID from the source if one exists; otherwise `f"NOID-{uuid.uuid4()}"`.
- `currency`: map to the `Currency` enum; add a new enum value if the currency is not yet listed.

## Step 4 — Register in the source maps

File: `pipeline_ingestion/data_models/data_sources.py`

Add to **both** maps — missing either causes a silent data gap or a runtime error:
```python
SOURCE_MODEL_MAP[SourceType.XXX] = XxxTransaction
# and
get_source_adapter_map()[SourceType.XXX] = XxxAdapter()
```

## Step 5 — Document the landing zone folder

The landing stage auto-creates `LANDING_ZONE_DIR / source_type.value.lower()` from the enum value. No code change needed. Tell the user where to drop files:
```
lnd/xxx/   ← drop CSV/XLSX exports here before running the pipeline
```

## Step 6 — Check if the refined stage needs source-specific logic

File: `pipeline_ingestion/stages/transactions/stage_03_refined.py`

The generic `process_refined_source` handles most sources automatically. Add a source-specific block only if the source requires:
- Special row filtering (e.g. Revolut exchange pairs)
- Multi-row date inheritance (e.g. ZKB debit eBanking parent/detail rows)
- A non-standard CHF conversion path

If no special logic is needed, state this explicitly in the response.

## Step 7 — Check if postprocess needs cross-source dedup

File: `pipeline_ingestion/stages/transactions/stage_04_postprocess.py`

Add logic only if the new source creates transactions that duplicate entries from another source (e.g. a credit card monthly payment appearing on both the card statement and the linked bank account). If no dedup is needed, state this explicitly.

## Step 8 — Write tests

Hand off to the `testing` agent (or write inline) for:
- Data model unit tests covering each datetime format variant and nullable fields.
- `@pytest.mark.parametrize` test against the sample CSV in `tests/test_data/`.
- `to_unified()` roundtrip test.

Add the sample CSV to `tests/test_data/<source_name>.csv`.

## Step 9 — Run the ingestion pipeline and verify

User drops a sample file in `lnd/xxx/` and runs:
```
python -m swiss_exp_tracker.pipeline_ingestion.pipeline
```

Check that `ingested_files`, `transactions_lnd`, `transactions_raw`, and `transactions_rfn` show the expected row counts. Verify `enrichment_status = "pending"` rows exist in `transactions_rfn`.

## Step 9b — Run the agentic pipeline on the new transactions

The agentic pipeline must run before any dashboard work. It enriches `transactions_rfn` rows with category labels — without this step the dashboard pipeline will produce empty or incomplete category data for the new source.

```
python -m swiss_exp_tracker.pipeline_agentic.pipeline
```

Verify that `enrichment_status` has changed from `"pending"` to `"done"` for the new rows in `transactions_rfn`. If any rows remain `"pending"` or show `"failed"`, investigate before proceeding — failed enrichment means those transactions will be invisible or miscategorised in the dashboard.

## Step 9c — Run the dashboard pipeline

After enrichment, rebuild the `dash_*` tables so the dashboard picks up the new transactions:

```
python -m swiss_exp_tracker.pipeline_dash.pipeline
```

Verify that the affected `dash_*` tables (e.g. `dash_balance`, `dash_cat_main`, `dash_top_expenses`) have increased row counts or updated values reflecting the new source. The dashboard reads exclusively from these tables — skipping this step means the new data will not appear in the app even if the ingestion and enrichment ran correctly.

## Step 10 — Dashboard integration

- If the source introduces new merchant names or categories needing specific colors, add them to `VIS.vk_*_col` in `app/config.py`.
- The home page loader reads all sources from `transactions_rfn` without source filtering — no change needed unless the source must be excluded.
- If a dedicated dashboard page is warranted, follow the `new-page` command after this agent finishes.

## After all steps

```
python -m ruff check <modified files>
python -m black --check <modified files>
python -m mypy <modified files> --ignore-missing-imports
```
