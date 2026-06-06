"""Run the Dash app with scrambled data for safe screenshots.

Copies both databases to temp files, multiplies every CHF/amount/price column
by a per-row random factor, patches the path constants *before* any app module
imports them, then starts the server. Real data is never touched or exposed.

Usage:
    python scripts/run_demo.py
"""

from __future__ import annotations

import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TRANSACTIONS_DB = ROOT / "database" / "transactions.db"
POSITIONS_DB = ROOT / "database" / "positions.db"

sys.path.insert(0, str(ROOT / "src"))

# Columns whose values must never be altered (keys, flags, dates, text, counts)
_SKIP_COLS = {
    "id",
    "transaction_id",
    "rfn_id",
    "source_type",
    "date",
    "time",
    "transaction_type",
    "currency",
    "reference",
    "merchant",
    "category_main",
    "category_second",
    "article",
    "unit",
    "location",
    "city",
    "processed",
    "year",
    "month",
    "symbol",
    "name",
}


def _scramble(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Multiply every numeric column not in _SKIP_COLS by a per-row random factor."""
    df = df.copy()
    for col in df.select_dtypes(include="number").columns:
        if col in _SKIP_COLS:
            continue
        factors = rng.uniform(0.45, 1.55, size=len(df))
        df[col] = (df[col] * factors).round(4)
    return df


def _scramble_db(src: Path, dst: Path, rng: np.random.Generator) -> None:
    """Copy src to dst and scramble all numeric non-key columns in every table."""
    shutil.copy(src, dst)
    with sqlite3.connect(str(dst)) as con:
        tables: list[str] = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            con,
        )["name"].tolist()
        for table in tables:
            df = pd.read_sql(f'SELECT * FROM "{table}"', con)
            if df.empty:
                continue
            df = _scramble(df, rng)
            df.to_sql(table, con, if_exists="replace", index=False)


def main() -> None:
    """Create demo DBs, patch path constants, and serve the app with fake data."""
    rng = np.random.default_rng()  # fresh random seed every run

    tmp_dir = tempfile.mkdtemp(prefix="demo_db_")
    demo_transactions = Path(tmp_dir) / "transactions_demo.db"
    demo_positions = Path(tmp_dir) / "positions_demo.db"

    print("Creating scrambled database copies...")
    _scramble_db(TRANSACTIONS_DB, demo_transactions, rng)
    if POSITIONS_DB.exists():
        _scramble_db(POSITIONS_DB, demo_positions, rng)
    else:
        sqlite3.connect(str(demo_positions)).close()

    # --- Patch path constants BEFORE any app module is imported for the first
    # time. loader.py binds DB_PATH at module load, so patching here ensures it
    # gets the demo path when loader.py is first imported by app.py. ---
    import swiss_exp_tracker.app.config as app_cfg
    import swiss_exp_tracker.pipeline_ingestion.config as ing_cfg

    app_cfg.DB_PATH = demo_transactions
    ing_cfg.INGESTION_DB_PATH = demo_transactions
    ing_cfg.POSITIONS_DB_PATH = demo_positions

    # Importing app triggers: run_dashboard_pipeline() → DataLoader() →
    # PositionsLoader() → create_app_factory() — all using the patched paths.
    from swiss_exp_tracker.app.app import app  # noqa: PLC0415

    print(f"\nDemo app ready — open http://127.0.0.1:8050")
    print(f"Temp DBs: {tmp_dir}\n")
    app.run(debug=False)


if __name__ == "__main__":
    main()
