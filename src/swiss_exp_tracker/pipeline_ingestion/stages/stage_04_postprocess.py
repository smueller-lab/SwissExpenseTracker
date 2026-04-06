from __future__ import annotations

import sqlite3

from collections import defaultdict

from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection


def _clean_credit_card_payments(db: sqlite3.Connection) -> int:
    zkb_rows = db.execute(
        """
        SELECT id, amount
        FROM transactions_refined
        WHERE source_type = ?
          AND transaction_type = ?
          AND (
                booking_text LIKE '%Viseca Payment%'
             OR booking_text LIKE '%Debit from LSV%'
          )
        ORDER BY amount, COALESCE(date, ''), id
        """,
        (SourceType.ZKB_DEBIT.value, TransactionType.EXPENSE.value),
    ).fetchall()

    viseca_rows = db.execute(
        """
        SELECT id, amount
        FROM transactions_refined
        WHERE source_type = ?
          AND transaction_type = ?
          AND (
                booking_text IS NULL
             OR TRIM(booking_text) = ''
          )
        ORDER BY amount, COALESCE(date, ''), id
        """,
        (SourceType.VISECA.value, TransactionType.INCOME.value),
    ).fetchall()

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

    placeholders = ",".join("?" for _ in ids_to_delete)
    db.execute(
        f"DELETE FROM transactions_refined WHERE id IN ({placeholders})",
        tuple(ids_to_delete),
    )
    return len(ids_to_delete) // 2


def _fill_viseca_credit_card_fee_text(db: sqlite3.Connection) -> int:
    cursor = db.execute(
        """
        UPDATE transactions_refined
        SET booking_text = ?,
            merchant_normalized = ?
        WHERE source_type = ?
          AND transaction_type = ?
          AND amount = ?
          AND (
                booking_text IS NULL
             OR TRIM(booking_text) = ''
          )
          AND merchant_normalized IS NULL
        """,
        (
            "Credit card fee",
            "credit card fee",
            SourceType.VISECA.value,
            TransactionType.EXPENSE.value,
            2.0,
        ),
    )
    return max(cursor.rowcount, 0)


def run_postprocess() -> dict[str, int]:
    create_all_tables()

    with get_connection() as db:
        credit_card_pairs_removed = _clean_credit_card_payments(db)
        viseca_fee_rows_updated = _fill_viseca_credit_card_fee_text(db)

    return {
        "credit_card_pairs_removed": credit_card_pairs_removed,
        "viseca_fee_rows_updated": viseca_fee_rows_updated,
    }
