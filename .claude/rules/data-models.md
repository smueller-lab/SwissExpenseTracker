---
name: data-models
description: Pydantic, Pandas, and SQLite conventions — the layer boundaries and type safety rules for pipeline and app code.
metadata:
  type: rules
---

# Data Model Rules

## Layer boundaries

| Layer | Format | Where used |
|-------|--------|-----------|
| Pipeline ingestion | Pydantic `BaseModel` | parsing raw CSV rows, adapter output |
| In-memory app | `pd.DataFrame` | loaders, figure functions, callbacks |
| Persistence | SQLite via `get_connection()` | DB reads/writes in pipeline stages |

- Never pass a Pydantic model to a figure function — convert to DataFrame first.
- Never write raw SQL in layout or callback files — DB access belongs in `loader.py` / `loader_positions.py`.

## Pydantic rules

- Use `BaseModel` for all pipeline data objects. Not plain dataclasses or TypedDicts.
- All fields must have explicit type annotations.
- Use `model_validator` / `field_validator` instead of `__init__` overrides.
- Never use `.dict()` — use `.model_dump()` (Pydantic v2).
- Never use `.parse_obj()` — use `Model.model_validate()` (Pydantic v2).
- `Optional[X]` fields must default to `None`.
- Every new transaction model needs `@field_validator` for date columns and all nullable numeric/string fields — no silent `None` coercion without an explicit validator.

## Column naming conventions

- DataFrame columns: `snake_case`.
- CHF amounts: always suffixed `_CHF` (app DataFrames) or `_chf` (DB columns).
- Period columns: ISO string `"YYYY-MM-01"` for monthly, `int` for yearly — never `pd.Period` (not JSON-serializable).

## Source registration

When a new `SourceType` enum value is added, it must have a registered `SourceProfile` (loaded via `load_profiles()` into `SUPPORTED_SOURCES` in `data_models/data_sources.py`).

Missing registration causes a runtime `NotImplementedError` from `get_profile()`.

## Adapter contract

- `adapters/generic_adapter.py::to_unified()` must always set `amount = abs(value)` — amounts in `UnifiedTransaction` are always positive.
- `transaction_type` is derived from the sign of the original value, not from the absolute amount.
- `reference_id` must be a stable unique ID; fall back to `f"NOID-{uuid.uuid4()}"` only if the source provides none.
