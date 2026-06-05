from __future__ import annotations

from pathlib import Path
from typing import Any

import aiosql


_QUERIES_DIR = Path(__file__).parent / "queries"

transactions: Any = aiosql.from_path(
    _QUERIES_DIR / "transactions.sql", "sqlite3", mandatory_parameters=False
)
groceries: Any = aiosql.from_path(
    _QUERIES_DIR / "groceries.sql", "sqlite3", mandatory_parameters=False
)
positions: Any = aiosql.from_path(
    _QUERIES_DIR / "positions.sql", "sqlite3", mandatory_parameters=False
)
agentic: Any = aiosql.from_path(
    _QUERIES_DIR / "agentic.sql", "sqlite3", mandatory_parameters=False
)
