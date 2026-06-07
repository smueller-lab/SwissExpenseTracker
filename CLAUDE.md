# Claude Code Guidelines

## Linting and Type Checking

Before finishing any task that modifies Python files, always run:

```bash
poetry run python -m ruff check <file>
poetry run python -m black --check <file>
poetry run mypy --config-file pyproject.toml src
```

Fix all reported issues before marking the task complete.

## Docstrings

Every function and method gets a docstring naming what it does, its inputs, and its output. Maximum 3 lines — use only as many as needed. No multi-paragraph blocks, no restating the function name.

```python
def get_ryAxis(d_Tick: float, z: pd.Series, q_ZeroStart: bool = False) -> list[float]:
    """Return [y_min, y_max] snapped to d_Tick boundaries; starts at 0 if q_ZeroStart."""
```

Don't pad to 3 lines — a single line is fine when the signature is self-explanatory. Extra lines are for non-obvious constraints, workarounds, or invariants only.

## Pydantic

- Always `BaseModel` — not plain dataclasses or `TypedDict`.
- All fields must have explicit type annotations.
- Use `model_validate()`, never `.parse_obj()`.
- Use `model_dump()`, never `.dict()`.
- Use `@field_validator` / `@model_validator` — never override `__init__`.
- `Optional[X]` fields must default to `None`.
- Every date and nullable numeric/string field needs an explicit `@field_validator` — no silent coercion.

## Commands

| Command | What it does |
|---------|-------------|
| `/team-plan <feature>` | Creates a `plan-<feature>.md` in the project root describing builder, tester, and validator tasks with files, criteria, and constraints |
| `/team-build <plan-file>` | Reads a plan file, builds the task graph, and orchestrates builder → tester → validator agents until the validator reports PASS |
| `/new-page <page-name>` | Scaffolds a new dashboard page — layout, callbacks, app registration, and a stub data loader method |
| `/new-data-source` | Guides adding a new bank or card CSV export end-to-end — from raw file format to dashboard visibility |

## Team Workflow

> **Trigger rule:** Whenever the user says *"create a plan for …"*, *"make a plan for …"*, or *"plan …"* for a feature, immediately invoke the `/team-plan` skill with the feature description as the argument. Do **not** write a plan file manually. The skill produces the structured builder/tester/validator format that `/team-build` requires.

Use `/team-plan <feature>` to create a plan file, then `/team-build <plan-file>` to execute it. The build command orchestrates three agents in sequence:

| Agent | Role | Constraint |
|-------|------|------------|
| `builder` | Writes production code only | Never touches test files |
| `tester` | Writes pytest tests only | Never modifies production code |
| `validator` | Runs ruff, black, mypy, pytest, and convention scan | Never modifies any file — reports only |

Dependency order: builder(s) → tester → validator. If the validator reports failures, new builder/tester fix tasks are created and the cycle repeats until PASS.
