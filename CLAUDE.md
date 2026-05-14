# SwissExpenseTracker — Project Guide

Swiss personal finance tool that ingests bank CSV exports, enriches transactions
with merchant metadata via an agentic web-search pipeline, and visualises the
results in a Plotly Dash dashboard.

---

## Running the Pipeline

### Full pipeline (all stages)
```bash
poetry run python src/swiss_exp_tracker/pipeline.py
```

Runs the four stages in sequence:
1. **Ingestion** — landing → raw → refined → postprocess
2. **Agentic enrichment** — web search + LLM categorisation (up to 2 000 pending tx per run)
3. **Post-clean** — apply manual corrections → `merchant_metadata_rfn`
4. **transactions_use** — final analysis-ready join table

### Run a single stage
```python
# Stage 1 — ingestion only
from swiss_exp_tracker.pipeline_ingestion.pipeline import run_ingestion
run_ingestion()

# Stage 2 — enrichment only
from swiss_exp_tracker.pipeline_agentic.pipeline import load_pending_transactions, run_all_transactions
import asyncio
txs = load_pending_transactions()
asyncio.run(run_all_transactions(txs))

# Stage 3 — post-clean only
from swiss_exp_tracker.pipeline_agentic.clean_pipeline_output import run_post_clean
run_post_clean()

# Stage 4 — rebuild transactions_use
from swiss_exp_tracker.pipeline_agentic.transactions_use import run_transactions_use
run_transactions_use()
```

---

## Running the Dashboard

```bash
poetry run python app/app.py
```

Opens at http://localhost:8050. The dashboard reads from `database/transactions.db`
(the `transactions_use` table). Run the full pipeline first to populate it.

---

## Running the Tests

```bash
poetry run pytest
```

VCR cassettes for web-search tests live in `tests/test_web_search/cassettes/`.
To re-record a cassette (requires real API keys), delete the relevant `.yaml` file
and run the test with the appropriate key set in `.env`.

---

## Dev Docs

Detailed technical documentation for each subsystem lives in `.dev-docs/`:

| File | Covers |
|------|--------|
| [`.dev-docs/01-agentic-pipeline.md`](.dev-docs/01-agentic-pipeline.md) | Agentic enrichment pipeline — agents, web-search fallback chain, vector store cache, DB tables |
| [`.dev-docs/02-ingestion-pipeline.md`](.dev-docs/02-ingestion-pipeline.md) | Ingestion pipeline — landing zone, 4 stages, source adapters, merchant normalisation, DB tables |

---

## Code Rules & Conventions

### Type checking — mypy
- mypy is configured and must pass with no errors.
- Always annotate function signatures (parameters + return types).
- Use `from __future__ import annotations` at the top of every module.
- Prefer `X | Y` union syntax (Python 3.10+) over `Optional[X]` or `Union[X, Y]`.
- Use `TYPE_CHECKING` guards for imports only needed at type-check time.

### Pydantic
- All data-model classes must inherit from `pydantic.BaseModel`.
- Use `ConfigDict` for model configuration (not the deprecated `class Config`).
- Use `Field(...)` for aliases, descriptions, and defaults — not bare assignments.
- Use `field_validator` / `model_validator` for validation logic; keep validators pure.
- Prefer `model_validate(dict)` over direct `__init__` calls when building from raw data.
- Never bypass validation with `model_construct` unless there is a strong performance justification.

### Enums
- Category enums (`CategoryMain`, `CategorySecond`) are `StrEnum` — use `.value` only
  when writing to the DB or external output; compare with enum members elsewhere.
- Never hardcode category strings; always reference the enum.

### Database (SQLite)
- Every table creation uses `CREATE TABLE IF NOT EXISTS`.
- Schema migrations require a manual `DROP TABLE` + re-run; there is no migration framework.
- Use parameterised queries (`?` placeholders) for all user/external data — no f-string SQL.
- `transactions_use` is a derived table — it is safe to drop and rebuild at any time.
- Credit usage is tracked per-provider per-month in `api_usage`; update `credit_limit`
  when a plan changes.

### Linting / formatting
- **ruff** for linting and import sorting (`poetry run ruff check .`).
- **black** for formatting (`poetry run black .`).
- All imports are absolute (relative imports are banned via ruff `TID252`).
- Imports are single-line (`force-single-line = true`).

### General
- Python ≥ 3.12.
- Use `pathlib.Path` for file paths, not `os.path` in new code.
- Async code uses `asyncio`; concurrency in the enrichment pipeline is controlled via
  `asyncio.Semaphore` (default concurrency = 5).
- `tqdm.write()` for progress messages inside loops — never `print()`.
