from __future__ import annotations

import sqlite3

from collections.abc import Sequence
from pathlib import Path

from swiss_exp_tracker.db.sql import agentic
from swiss_exp_tracker.db.sql import groceries
from swiss_exp_tracker.db.sql import transactions
from tests.fixtures.models import GroceryUseFixture
from tests.fixtures.models import UseTransactionFixture


def build_fixture_db(
    db_path: Path,
    use_rows: Sequence[UseTransactionFixture],
    grocery_rows: Sequence[GroceryUseFixture],
) -> Path:
    """Create a fixture SQLite DB at db_path, populate it, and return db_path.

    Creates transactions_use, transactions_rfn (empty), and groceries_use tables
    then inserts use_rows (batch) and grocery_rows (per-row) via aiosql only.
    """
    with sqlite3.connect(db_path) as con:
        agentic.create_transactions_use_table(con)
        transactions.create_transactions_rfn_table(con)
        groceries.create_groceries_use_table(con)

        agentic.insert_transactions_use(con, [r.to_insert_dict() for r in use_rows])

        for row in grocery_rows:
            groceries.insert_groceries_use(con, **row.to_insert_dict())

        con.commit()

    return db_path
