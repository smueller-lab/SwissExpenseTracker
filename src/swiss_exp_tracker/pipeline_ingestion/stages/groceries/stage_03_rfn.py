from __future__ import annotations

import json

from collections import defaultdict
from datetime import datetime
from typing import NamedTuple

from tqdm import tqdm

from swiss_exp_tracker.db.sql import groceries
from swiss_exp_tracker.db.sql import transactions
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


def run_groceries_rfn() -> dict[str, int]:
    """Promote unprocessed groceries_raw rows to groceries_rfn with netting and deduplication."""
    create_grocery_tables()

    with get_connection() as db:
        rows = list(
            groceries.get_unprocessed_groceries_raw_rows(
                db, source_type=_SOURCE_TYPE.value
            )
        )

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
                is_duplicate = bool(
                    groceries.check_duplicate_groceries_rfn(
                        db,
                        date=date_iso,
                        time=time_iso,
                        location=location,
                        article=netted_item.article,
                        quantity=netted_item.quantity,
                    )
                )
                if is_duplicate:
                    duplicates_skipped += 1
                    continue

                # raw_id is NULL for multi-source netted rows, single row otherwise.
                raw_id_val = (
                    netted_item.raw_ids[0] if len(netted_item.raw_ids) == 1 else None
                )

                groceries.insert_groceries_rfn(
                    db,
                    raw_id=raw_id_val,
                    source_type=_SOURCE_TYPE.value,
                    date=date_iso,
                    time=time_iso,
                    location=location,
                    article=netted_item.article,
                    article_normalized=netted_item.article_normalized,
                    unit=netted_item.unit,
                    quantity=netted_item.quantity,
                    price_chf=netted_item.price,
                    discount_chf=netted_item.discount,
                    created_at=datetime.now().isoformat(),
                )
                records_inserted += 1

        # Mark every raw row processed regardless of netting outcome.
        # Collect file_ids here to avoid a large IN-clause query later
        # (SQLite caps host parameters at 999).
        for raw_id in raw_ids_all:
            groceries.mark_groceries_raw_processed(db, raw_id=raw_id)
            file_id_val = groceries.get_file_id_for_groceries_raw_row(db, raw_id=raw_id)
            if file_id_val is not None:
                processed_file_ids.add(int(file_id_val))

        # Advance file status when all raw rows for a file are done.
        for file_id in processed_file_ids:
            count = groceries.count_unprocessed_groceries_raw_for_file_full(
                db, file_id=file_id
            )
            if not count:
                transactions.set_ingested_file_status(
                    db, status="refined", file_id=file_id
                )

    return {
        "rows_found": len(rows),
        "rows_processed": len(raw_ids_all),
        "records_inserted": records_inserted,
        "duplicates_skipped": duplicates_skipped,
    }
