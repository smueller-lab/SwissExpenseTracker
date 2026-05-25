from __future__ import annotations

import json

from collections import defaultdict
from datetime import datetime
from typing import NamedTuple

from tqdm import tqdm

from swiss_exp_tracker.pipeline_ingestion.data_models.grocery import GroceryItem
from swiss_exp_tracker.pipeline_ingestion.data_models.grocery import normalize_article
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.db_groceries import create_grocery_tables


_SOURCE_TYPE = SourceType.MIGROS_GROCERY


class _ReceiptItem(NamedTuple):
    raw_id: int
    article: str
    article_normalized: str
    quantity: float
    price: float
    discount: float
    unit: str


class _NettedItem(NamedTuple):
    raw_ids: list[int]
    article: str
    article_normalized: str
    quantity: float
    price: float
    discount: float
    unit: str


def net_receipt_rows(items: list[_ReceiptItem]) -> list[_NettedItem]:
    """Net return rows within a single receipt.

    Groups by article, sums quantity and price. Returns only items whose net
    quantity is positive. Zero-net items (full returns) are silently discarded.
    Negative-net items (data anomaly) are discarded with a warning.
    """
    by_article: dict[str, list[_ReceiptItem]] = defaultdict(list)
    for item in items:
        by_article[item.article].append(item)

    result: list[_NettedItem] = []
    for article, article_items in by_article.items():
        net_qty = round(sum(i.quantity for i in article_items), 6)
        net_price = round(sum(i.price for i in article_items), 2)
        net_discount = round(sum(i.discount for i in article_items), 2)
        raw_ids = [i.raw_id for i in article_items]
        normalized = article_items[0].article_normalized
        unit = article_items[0].unit

        if net_qty == 0.0:
            continue
        if net_qty < 0.0:
            tqdm.write(
                f"[WARN] Net quantity {net_qty} < 0 for article '{article}' — discarding"
            )
            continue

        result.append(
            _NettedItem(
                raw_ids=raw_ids,
                article=article,
                article_normalized=normalized,
                quantity=net_qty,
                price=net_price,
                discount=net_discount,
                unit=unit,
            )
        )

    return result


def _is_duplicate(
    db_conn: object,
    date: str,
    time: str,
    location: str,
    article: str,
    quantity: float,
) -> bool:
    import sqlite3

    conn: sqlite3.Connection = db_conn  # type: ignore[assignment]
    row = conn.execute(
        """
        SELECT 1 FROM groceries_rfn
        WHERE date = ? AND time = ? AND location = ? AND article = ? AND quantity = ?
        LIMIT 1
        """,
        (date, time, location, article, quantity),
    ).fetchone()
    return row is not None


def run_groceries_rfn() -> dict[str, int]:
    create_grocery_tables()

    with get_connection() as db:
        rows = db.execute(
            """
            SELECT gr.id, gr.landing_id, gl.file_id, gr.raw_json, gr.source_file
            FROM groceries_raw gr
            JOIN groceries_lnd gl ON gl.id = gr.landing_id
            WHERE gr.processed = 0
              AND gr.source_type = ?
            ORDER BY gr.id
            """,
            (_SOURCE_TYPE.value,),
        ).fetchall()

    if not rows:
        return {
            "rows_found": 0,
            "rows_processed": 0,
            "records_inserted": 0,
            "duplicates_skipped": 0,
        }

    # Parse all rows and group by receipt key (date, time, location).
    # Bonus rows are discarded here; return rows go through netting.
    receipt_items: dict[tuple[str, str, str], list[_ReceiptItem]] = defaultdict(list)
    raw_ids_all: list[int] = []

    for raw_id, _landing_id, _file_id, raw_json, _source_file in rows:
        item = GroceryItem.model_validate(json.loads(raw_json))
        raw_ids_all.append(raw_id)

        if item.price == 0 and item.discount == 0:  # bonus row
            continue

        receipt_key: tuple[str, str, str] = (
            item.date.isoformat(),
            item.time.isoformat(),
            item.location,
        )
        abs_qty = abs(item.quantity)
        unit_val = "kg" if abs_qty != int(abs_qty) else "qty"
        receipt_items[receipt_key].append(
            _ReceiptItem(
                raw_id=raw_id,
                article=item.article,
                article_normalized=normalize_article(item.article),
                quantity=item.quantity,
                price=item.price,
                discount=item.discount,
                unit=unit_val,
            )
        )

    records_inserted = 0
    duplicates_skipped = 0
    processed_file_ids: set[int] = set()

    with get_connection() as db:
        for (date_iso, time_iso, location), receipt in receipt_items.items():
            netted = net_receipt_rows(receipt)

            for netted_item in netted:
                if _is_duplicate(
                    db,
                    date_iso,
                    time_iso,
                    location,
                    netted_item.article,
                    netted_item.quantity,
                ):
                    duplicates_skipped += 1
                    continue

                # raw_id is NULL for multi-source netted rows, single row otherwise.
                raw_id_val = (
                    netted_item.raw_ids[0] if len(netted_item.raw_ids) == 1 else None
                )

                db.execute(
                    """
                    INSERT INTO groceries_rfn (
                        raw_id, source_type, date, time, location,
                        article, article_normalized, unit,
                        quantity, price_chf, discount_chf,
                        enrichment_status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                    """,
                    (
                        raw_id_val,
                        _SOURCE_TYPE.value,
                        date_iso,
                        time_iso,
                        location,
                        netted_item.article,
                        netted_item.article_normalized,
                        netted_item.unit,
                        netted_item.quantity,
                        netted_item.price,
                        netted_item.discount,
                        datetime.now().isoformat(),
                    ),
                )
                records_inserted += 1

        # Mark every raw row processed regardless of netting outcome.
        # Collect file_ids here to avoid a large IN-clause query later
        # (SQLite caps host parameters at 999).
        for raw_id in raw_ids_all:
            db.execute(
                "UPDATE groceries_raw SET processed = 1 WHERE id = ?",
                (raw_id,),
            )
            file_id_row = db.execute(
                """
                SELECT gl.file_id
                FROM groceries_raw gr
                JOIN groceries_lnd gl ON gl.id = gr.landing_id
                WHERE gr.id = ?
                """,
                (raw_id,),
            ).fetchone()
            if file_id_row:
                processed_file_ids.add(int(file_id_row[0]))

        # Advance file status when all raw rows for a file are done.
        for file_id in processed_file_ids:
            unprocessed = db.execute(
                """
                SELECT COUNT(*) FROM groceries_raw gr
                JOIN groceries_lnd gl ON gl.id = gr.landing_id
                WHERE gl.file_id = ? AND gr.processed = 0
                """,
                (file_id,),
            ).fetchone()
            if unprocessed and int(unprocessed[0]) == 0:
                db.execute(
                    "UPDATE ingested_files SET status = 'refined' WHERE id = ?",
                    (file_id,),
                )

    return {
        "rows_found": len(rows),
        "rows_processed": len(raw_ids_all),
        "records_inserted": records_inserted,
        "duplicates_skipped": duplicates_skipped,
    }
