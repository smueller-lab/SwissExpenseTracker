from __future__ import annotations

import sqlite3

from collections import defaultdict

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection


def _clean_credit_card_payments(db: sqlite3.Connection) -> int:
    """Remove matched ZKB-Viseca payment pairs from transactions_rfn; returns pair count removed."""
    zkb_rows = transactions.get_zkb_credit_card_payment_rows(
        db,
        source_type=SourceType.ZKB_DEBIT.value,
        transaction_type=TransactionType.EXPENSE.value,
    )

    viseca_rows = transactions.get_viseca_income_rows(
        db,
        source_type=SourceType.VISECA.value,
        transaction_type=TransactionType.INCOME.value,
    )

    zkb_by_amount: dict[float, list[int]] = defaultdict(list)
    for row in zkb_rows:
        zkb_by_amount[float(row[1])].append(int(row[0]))

    viseca_by_amount: dict[float, list[int]] = defaultdict(list)
    for row in viseca_rows:
        viseca_by_amount[float(row[1])].append(int(row[0]))

    ids_to_delete: list[int] = []
    for amount, zkb_ids in zkb_by_amount.items():
        viseca_ids = viseca_by_amount.get(amount)
        if not viseca_ids:
            continue

        match_count = min(len(zkb_ids), len(viseca_ids))
        ids_to_delete.extend(zkb_ids[:match_count])
        ids_to_delete.extend(viseca_ids[:match_count])

    if not ids_to_delete:
        return 0

    # dynamic IN clause: cannot be expressed as static aiosql SQL
    placeholders = ",".join("?" for _ in ids_to_delete)
    db.execute(
        f"DELETE FROM transactions_rfn WHERE id IN ({placeholders})",
        tuple(ids_to_delete),
    )
    return len(ids_to_delete) // 2


def _fill_viseca_credit_card_fee_text(db: sqlite3.Connection) -> int:
    """Backfill booking_text and merchant for Viseca fee rows; returns rows updated."""
    rowcount: int = transactions.fill_viseca_fee_text(
        db,
        booking_text="Credit card fee",
        merchant_normalized="credit card fee",
        source_type=SourceType.VISECA.value,
        transaction_type=TransactionType.EXPENSE.value,
        amount=2.0,
    )
    return max(rowcount, 0)


def run_postprocess() -> dict[str, int]:
    """Run postprocessing: remove credit card payment pairs and fill Viseca fee text."""
    create_all_tables()

    with get_connection() as db:
        credit_card_pairs_removed = _clean_credit_card_payments(db)
        viseca_fee_rows_updated = _fill_viseca_credit_card_fee_text(db)

    return {
        "credit_card_pairs_removed": credit_card_pairs_removed,
        "viseca_fee_rows_updated": viseca_fee_rows_updated,
    }
