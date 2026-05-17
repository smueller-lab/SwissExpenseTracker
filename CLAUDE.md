# Claude Code Guidelines

## Linting and Type Checking

Before finishing any task that modifies Python files, always run:

```bash
python -m ruff check <file>
python -m black --check <file>
python -m mypy <file> --ignore-missing-imports
```

Fix all reported issues before marking the task complete.
