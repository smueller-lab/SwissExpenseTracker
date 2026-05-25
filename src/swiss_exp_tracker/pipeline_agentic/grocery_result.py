from __future__ import annotations

import os
import sqlite3

from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryResult


oj = os.path.join

__all__ = ["GroceryResult"]

_DB_DIR = "./database"
_DB_PATH = oj(_DB_DIR, "transactions.db")


class GroceryResult:
    def __init__(self) -> None:
        os.makedirs(_DB_DIR, exist_ok=True)
        self._create_tables()

    def _create_tables(self) -> None:
        with sqlite3.connect(_DB_PATH) as db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS grocery_categorization_raw (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at      TEXT NOT NULL,
                    rfn_id          INTEGER NOT NULL,
                    article         TEXT NOT NULL,
                    matched_article TEXT NOT NULL,
                    cache_hit       INTEGER NOT NULL,
                    similarity      REAL,
                    category_main   TEXT NOT NULL,
                    category_detail TEXT NOT NULL
                )
                """
            )
            db.execute(
                """
                CREATE VIEW IF NOT EXISTS grocery_categorization_rfn AS
                SELECT * FROM grocery_categorization_raw
                WHERE id = (
                    SELECT MAX(id)
                    FROM grocery_categorization_raw g2
                    WHERE g2.rfn_id = grocery_categorization_raw.rfn_id
                )
                """
            )
            db.commit()

    def save_grocery_result(self, result: GroceryCategoryResult) -> None:
        with sqlite3.connect(_DB_PATH) as db:
            db.execute(
                """
                INSERT INTO grocery_categorization_raw (
                    created_at, rfn_id, article, matched_article,
                    cache_hit, similarity, category_main, category_detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.current_datetime.isoformat(),
                    result.rfn_id,
                    result.article,
                    result.matched_article,
                    int(result.cache_hit),
                    result.similarity,
                    result.category_main.value,
                    result.category_detail.value,
                ),
            )
            db.commit()

    def mark_grocery_enriched(self, rfn_id: int) -> None:
        with sqlite3.connect(_DB_PATH) as db:
            db.execute(
                "UPDATE groceries_rfn SET enrichment_status = 'enriched' WHERE id = ?",
                (rfn_id,),
            )
            db.commit()
