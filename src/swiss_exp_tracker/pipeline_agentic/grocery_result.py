from __future__ import annotations

import os
import sqlite3

from swiss_exp_tracker.db.sql import groceries
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
        """Create grocery_categorization_raw table and rfn view if absent."""
        with sqlite3.connect(_DB_PATH) as db:
            groceries.create_grocery_categorization_raw_table(db)
            groceries.create_grocery_categorization_rfn_view(db)
            db.commit()

    def save_grocery_result(self, result: GroceryCategoryResult) -> None:
        """Persist one grocery categorization result to the DB."""
        with sqlite3.connect(_DB_PATH) as db:
            groceries.insert_grocery_categorization_raw(
                db,
                created_at=result.current_datetime.isoformat(),
                rfn_id=result.rfn_id,
                article=result.article,
                matched_article=result.matched_article,
                cache_hit=int(result.cache_hit),
                similarity=result.similarity,
                category_main=result.category_main.value,
                category_detail=result.category_detail.value,
            )
            db.commit()

    def mark_grocery_enriched(self, rfn_id: int) -> None:
        """Set enrichment_status = 'enriched' for the given groceries_rfn row id."""
        with sqlite3.connect(_DB_PATH) as db:
            groceries.mark_grocery_enriched(db, rfn_id=rfn_id)
            db.commit()
