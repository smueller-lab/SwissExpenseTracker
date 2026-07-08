# Contributing to Swiss Expense Tracker

Thanks for your interest in contributing! This doc covers the local setup, the
conventions CI enforces, and how to submit a change.

## Getting set up

See [README → 🚀 Getting Started](README.md#-getting-started) for the full
install path, including the **[Dev Container](README.md#-develop-in-a-container)**
option, which gives you the pinned Python 3.12, Poetry, and the whole dev
toolchain (ruff, black, mypy, pytest) with no host setup beyond Docker.

Quick path with Poetry directly:

```bash
git clone https://github.com/smueller-lab/SwissExpenseTracker.git
cd SwissExpenseTracker
pip install poetry && poetry install
```

## Before opening a PR

CI (`.github/workflows/ci.yml`) runs lint, formatting, type checks, tests, and
a Docker build on every PR to `main`. Run the same checks locally first:

```bash
poetry run ruff check src tests
poetry run black --check src tests
poetry run mypy --config-file pyproject.toml
poetry run pytest --cov --cov-report=term-missing
```

Fix everything these report before pushing — a red CI run blocks review.

## Code conventions

Full detail lives in `.claude/rules/` and `CLAUDE.md`; the highlights:

- **Docstrings**: every function/method gets a docstring naming what it does,
  its inputs, and its output — max 3 lines, no padding.
- **No unused parameters**: every declared parameter must be used in the
  function body; prefix genuinely-unused framework-mandated args with `_`.
- **Typing**: no `getattr`/`hasattr` fallback chains for "might be one of
  several types" — type the parameter as a union and narrow with `isinstance`.
- **Pydantic**: `BaseModel` everywhere (not dataclasses/`TypedDict`),
  `model_validate()`/`model_dump()` (v2 API, never `.parse_obj()`/`.dict()`),
  validators instead of `__init__` overrides, explicit `@field_validator` on
  every date/nullable field.
- **Layer boundaries**: Pydantic models for pipeline ingestion, `pd.DataFrame`
  in the app layer, SQLite only behind `loader.py`/`loader_positions.py` — see
  [`.claude/rules/data-models.md`](.claude/rules/data-models.md).
- **Dash layout**: no inline `style={}`, cards use `components/cards.py`
  helpers, grid rows must sum to `12` — see
  [`.claude/rules/dash-layout.md`](.claude/rules/dash-layout.md).
- **Plotly figures**: shared theme, axis, and sizing helpers are mandatory —
  see [`.claude/rules/plots.md`](.claude/rules/plots.md).

## Tests

- Framework: `pytest` + `pytest-cov` + `pytest-asyncio` (`asyncio_mode = "auto"`,
  no `@pytest.mark.asyncio` needed) + `pytest-mock` (`mocker` fixture, not
  `unittest.mock` directly).
- Test files mirror source paths under `tests/` (e.g.
  `src/.../figure.py` → `tests/test_app_vis/test_figure.py`); name tests
  `test_{what}_{condition}`.
- Use a `_make_row(**overrides)` factory instead of hardcoding a single dict
  per test.
- Never touch `database/transactions.db` or `database/positions.db` — use the
  `tmp_db`/`tmp_path` fixtures to redirect DB paths.
- Use `pytest.approx` for float comparisons, never `==`.

Full conventions: [`.claude/rules/testing.md`](.claude/rules/testing.md).

## Adding a new bank/card source or dashboard page

Two guided workflows exist for the most common contribution shapes:

- New CSV/XLSX transaction source → `/new-data-source`
- New dashboard page → `/new-page <page-name>`

Both are Claude Code skills; if you're contributing by hand instead, read
through the relevant `.dev-docs/` chapter first (see
[README → 📚 Dev Docs](README.md#-dev-docs)) to find the files a new source or
page needs to touch.

## Commit / PR guidelines

- Keep PRs focused on one change; avoid bundling unrelated refactors.
- Write commit messages and PR descriptions around the *why*, not a restatement
  of the diff.
- Link the PR to an issue if one exists.
- Make sure `poetry run pytest` and the lint/type checks above are green
  before requesting review.

## Reporting bugs / requesting features

Open a GitHub issue with:

- What you expected vs. what happened.
- Steps to reproduce (sample input shape is fine — never attach real bank
  data/exports).
- Relevant log output or stack trace.

## License

By contributing, you agree that your contributions will be licensed under the
project's [GNU General Public License v3.0](LICENSE).
