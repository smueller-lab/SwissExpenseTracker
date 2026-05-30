# Claude Code Guidelines

## Linting and Type Checking

Before finishing any task that modifies Python files, always run:

```bash
python -m ruff check <file>
python -m black --check <file>
python -m mypy <file> --ignore-missing-imports
```

Fix all reported issues before marking the task complete.

## Docstrings

Every function and method gets a single-line docstring that names what it does, its inputs, and its output. Keep it to one line. No multi-paragraph blocks, no restating the function name.

```python
def get_ryAxis(d_Tick: float, z: pd.Series, q_ZeroStart: bool = False) -> list[float]:
    """Return [y_min, y_max] snapped to d_Tick boundaries; starts at 0 if q_ZeroStart."""
```

Add a second line only when there is a non-obvious constraint, workaround, or invariant a reader would not infer from the signature — not to describe what the code does line by line.

## Pydantic

- Always `BaseModel` — not plain dataclasses or `TypedDict`.
- All fields must have explicit type annotations.
- Use `model_validate()`, never `.parse_obj()`.
- Use `model_dump()`, never `.dict()`.
- Use `@field_validator` / `@model_validator` — never override `__init__`.
- `Optional[X]` fields must default to `None`.
- Every date and nullable numeric/string field needs an explicit `@field_validator` — no silent coercion.

## Agent Delegation

Always delegate to the appropriate sub-agent rather than handling these tasks inline. Doing the work inline bypasses the agent's rules and safety gates.

| Task | Agent |
|------|-------|
| Create or modify any Plotly figure / chart | `plotting` |
| Review or fix code quality, type errors, convention violations | `code-reviewer` |
| Write or extend tests for any component | `testing` |
| Add a new bank / card CSV data source end-to-end | `new-data-source` |
| Change pipeline ingestion, agentic categorisation, or dash pipeline | `pipeline` |
| Any SQLite schema change (CREATE TABLE, ALTER TABLE, DROP) | `db-schema` |
