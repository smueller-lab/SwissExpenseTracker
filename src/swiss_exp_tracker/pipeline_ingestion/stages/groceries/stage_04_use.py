from __future__ import annotations

from tqdm import tqdm

from swiss_exp_tracker.db.sql import groceries
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryDetail
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryMain
from swiss_exp_tracker.pipeline_agentic.grocery_result import GroceryResult
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.db_groceries import create_grocery_tables

# (article LIKE pattern, correct category_main, correct category_detail)
_CORRECTIONS: list[tuple[str, GroceryCategoryMain, GroceryCategoryDetail]] = [
    ("Aproz%", GroceryCategoryMain.BEVERAGES, GroceryCategoryDetail.WATER),
]


def _apply_corrections(db: object) -> int:
    """Apply hard-coded category corrections to groceries_use. Returns rows updated."""
    import sqlite3

    conn: sqlite3.Connection = db  # type: ignore[assignment]
    updated = 0
    for pattern, main, detail in _CORRECTIONS:
        rowcount = groceries.apply_groceries_use_category_correction(
            conn,
            category_main=main,
            category_detail=detail,
            pattern=pattern,
        )
        updated += rowcount
    return updated


def run_groceries_use() -> dict[str, int]:
    """Join groceries_rfn + grocery_categorization_rfn → groceries_use."""
    create_grocery_tables()
    # Ensure grocery_categorization_rfn VIEW exists before querying it.
    GroceryResult()

    with get_connection() as db:
        groceries.create_groceries_use_table(db)

        rows = list(groceries.get_groceries_for_use(db))

        for row in rows:
            groceries.insert_groceries_use(
                db,
                rfn_id=row[0],
                date=row[1],
                time=row[2],
                location=row[3],
                article=row[4],
                unit=row[5],
                quantity=row[6],
                price_chf=row[7],
                discount_chf=row[8],
                category_main=row[9],
                category_detail=row[10],
            )

        corrected = _apply_corrections(db)
        db.commit()

    tqdm.write(
        f"groceries_use: {len(rows)} rows written, {corrected} corrections applied"
    )
    return {"rows_written": len(rows), "rows_corrected": corrected}
