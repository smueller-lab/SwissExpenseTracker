---
name: testing
description: Test structure, DB isolation, factory pattern, and coverage expectations for every component type in this project.
metadata:
  type: rules
---

# Testing Rules

## Tooling

Always use:
- `pytest` — test runner
- `pytest-cov` — coverage
- `pytest-asyncio` — async tests (configured with `asyncio_mode = "auto"`)
- `pytest-mock` — mocking via `mocker` fixture (wraps `unittest.mock`)

Never use:
- `unittest` style classes — use plain `pytest` functions
- `mock` standalone package — use `pytest-mock`'s `mocker` fixture

Install: `uv add --dev pytest pytest-cov pytest-asyncio pytest-mock`

### pyproject.toml configuration

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
addopts = ["-ra", "-q", "--strict-markers", "--strict-config"]

[tool.coverage.run]
source = ["src"]
branch = true

[tool.coverage.report]
exclude_lines = ["pragma: no cover", "if TYPE_CHECKING:", "raise NotImplementedError"]
```

## Async tests

With `asyncio_mode = "auto"`, no `@pytest.mark.asyncio` decorator is needed — just `async def`:

```python
async def test_fetch_user():
    user = await fetch_user(1)
    assert user["id"] == 1

@pytest.fixture
async def async_client():
    async with AsyncClient() as client:
        yield client
```

Do NOT use `asyncio.run()` inside tests — pytest-asyncio handles the event loop. Use `asyncio.run()` only in production code or in test helpers that must be synchronous.

## Mocking

Use `mocker` from pytest-mock, not direct `unittest.mock.patch`:

```python
def test_send_email(mocker):
    mock_send = mocker.patch("mypackage.email.send_email", return_value=True)
    result = notify_user("test@example.com", "Hello")
    assert result is True
    mock_send.assert_called_once_with("test@example.com", "Hello")

async def test_external_api(mocker):
    mock_fetch = mocker.patch(
        "mypackage.client.fetch_data",
        new_callable=AsyncMock,
        return_value={"data": "mocked"},
    )
    result = await process_data()
    assert result["data"] == "mocked"
    mock_fetch.assert_awaited_once()
```

Use `monkeypatch.setattr` (pytest built-in) for module-level attribute patching (e.g. DB path constants, credit counters).

## File location and naming

Tests live under `tests/`. Each test module mirrors the source module it covers:

| Source file | Test file |
|-------------|-----------|
| `pipeline_ingestion/data_models/transaction.py` | `tests/test_data_models/test_transaction_data_models.py` |
| `pipeline_ingestion/stages/groceries/stage_03_rfn.py` | `tests/test_pipeline_groceries/test_stage_rfn.py` |
| `app/vis/figure.py` | `tests/test_app_vis/test_figure.py` |

- Test function names: `test_{what}_{condition}` — e.g. `test_rfn_skips_duplicate`, `test_zkb_date_iso_format_parsed`.
- Each test directory needs its own `conftest.py` (shared fixtures) and `__init__.py`.

## Factory pattern — mandatory

Always create a `_make_row(**overrides)` function that returns a valid base dict and accepts keyword overrides. Never hardcode a single magic dict inside an individual test — use the factory so each test only states what it varies.

```python
def _make_row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "Date": "17.05.2026",
        "Amount": "12.50",
        ...
    }
    base.update(overrides)
    return base
```

Real CSV/XLSX samples live in `tests/test_data/`. When adding a new source, add a representative sample file and parametrize against it.

## Database isolation — mandatory

Tests must never read from or write to `database/transactions.db` or `database/positions.db`.

- Use `tmp_path` (pytest built-in) + `monkeypatch.setattr` to redirect the pipeline DB path constant. Follow `tests/test_pipeline_groceries/conftest.py` exactly.
- The `tmp_db` fixture creates the full schema before each test. Schema definitions in fixtures must stay in sync with `db.py` / `db_groceries.py`.
- `_insert_file`, `_insert_landing`, `_insert_raw` helpers go in the test module, not conftest, unless shared across multiple test files.

## Numeric comparisons

Always use `pytest.approx` for floats. Never `==` on a float.

```python
assert result[0].price == pytest.approx(1.25)  # correct
assert result[0].price == 1.25                  # wrong
```

## What to cover per component type

### Pydantic data models
- Happy path: valid row parses, all fields populated.
- Each datetime format variant the source CSV produces — one test per format.
- Each nullable field receiving empty string, `None`, and missing key — model must not raise; field must be `None`.
- Comma decimal separator if applicable (`"1,80"` → `1.80`).
- `to_unified()` roundtrip: assert `isinstance(unified, UnifiedTransaction)`, `unified.amount >= 0`, `unified.transaction_type in TransactionType`, `unified.source_file` is non-empty.
- `@pytest.mark.parametrize` against real sample CSV: load up to 100 rows, fixed seed `42`, run full parse + adapter for each.

### Pipeline stage tests (with DB)
- `tmp_db` fixture via `tmp_path` + `monkeypatch.setattr`.
- Happy path: one valid row flows through and appears in the output table with correct values.
- Idempotency: second run returns `records_inserted == 0`, no duplicate rows.
- Processed flag: all input rows have `processed = 1` after the stage runs.
- Deduplication: identical rows from two files — only one in output.
- Stage-specific skip conditions (bonus rows, exchange legs, ZKB debit eBanking parent rows).
- Return dict counters (`rows_found`, `records_inserted`, `duplicates_skipped`) match expectations.

### Pure processing functions (no DB)
- Import the internal function directly and call it. No fixtures needed.
- Cover: normal input, empty input, boundary values, all logical branches.

### Dashboard pipeline tests
- Copy real DB to `tmp_path` with `shutil.copy` — never modify the real DB.
- Assert all expected tables exist, all expected columns present (`PRAGMA table_info`), all tables non-empty.
- `DataLoader(db_path=tmp_db)` initialises without error; all expected DataFrame attributes are not `None` and not empty.

### Plotly figure tests
- No Dash server needed.
- Build a minimal valid DataFrame matching the function's expected columns and dtypes.
- Assert: `isinstance(fig, go.Figure)`, `len(fig.data) >= 1`, y-axis range end is divisible by the expected `dTick` with no remainder, `fig.layout.height > 0`.
- Do not assert pixel-exact values or hex colour strings — test structure, not appearance.
