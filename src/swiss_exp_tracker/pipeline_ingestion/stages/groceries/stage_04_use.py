from __future__ import annotations

import sqlite3

from tqdm import tqdm

from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryDetail
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryMain
from swiss_exp_tracker.pipeline_agentic.grocery_result import GroceryResult
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.db_groceries import create_grocery_tables


# (article LIKE pattern, correct category_main, correct category_detail)
_CORRECTIONS: list[tuple[str, GroceryCategoryMain, GroceryCategoryDetail]] = [
    ("Aproz%", GroceryCategoryMain.BEVERAGES, GroceryCategoryDetail.WATER),
]


def _apply_corrections(db: sqlite3.Connection) -> int:
    """Apply hard-coded category corrections to groceries_use. Returns rows updated."""
    updated = 0
    for pattern, main, detail in _CORRECTIONS:
        cursor = db.execute(
            """
            UPDATE groceries_use
               SET category_main   = ?,
                   category_detail = ?
             WHERE article LIKE ?
               AND (category_main != ? OR category_detail != ?)
            """,
            (main, detail, pattern, main, detail),
        )
        updated += cursor.rowcount
    return updated


def run_groceries_use() -> dict[str, int]:
    """Join groceries_rfn + grocery_categorization_rfn → groceries_use."""
    create_grocery_tables()
    # Ensure grocery_categorization_rfn VIEW exists before querying it.
    GroceryResult()

    with get_connection() as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS groceries_use (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                rfn_id          INTEGER NOT NULL,
                date            TEXT NOT NULL,
                time            TEXT NOT NULL,
                location        TEXT NOT NULL,
                article         TEXT NOT NULL,
                unit            TEXT NOT NULL,
                quantity        REAL NOT NULL,
                price_chf       REAL NOT NULL,
                discount_chf    REAL NOT NULL,
                category_main   TEXT NOT NULL,
                category_detail TEXT NOT NULL
            )
            """
        )

        rows = db.execute(
            """
            SELECT
                r.id          AS rfn_id,
                r.date,
                r.time,
                r.location,
                r.article,
                r.unit,
                r.quantity,
                r.price_chf,
                r.discount_chf,
                c.category_main,
                c.category_detail
            FROM groceries_rfn r
            JOIN grocery_categorization_rfn c ON c.rfn_id = r.id
            WHERE r.enrichment_status = 'enriched'
              AND r.id NOT IN (SELECT rfn_id FROM groceries_use)
            ORDER BY r.date DESC, r.time DESC
            """
        ).fetchall()

        for row in rows:
            db.execute(
                """
                INSERT INTO groceries_use (
                    rfn_id, date, time, location, article,
                    unit, quantity, price_chf, discount_chf,
                    category_main, category_detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(row),
            )

        corrected = _apply_corrections(db)
        db.commit()

    tqdm.write(
        f"groceries_use: {len(rows)} rows written, {corrected} corrections applied"
    )
    return {"rows_written": len(rows), "rows_corrected": corrected}
