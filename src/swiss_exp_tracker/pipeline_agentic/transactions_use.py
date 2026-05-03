"""Build the final analysis table ``transactions_use``.

Joins ``transactions_rfn`` (enriched transactions) with ``merchant_metadata_rfn``
(cleaned merchant categories) on ``reference = zkb_reference`` and writes the
result into ``transactions_use``.

Only transactions whose ``enrichment_status = 'enriched'`` are included, and
only those not already present in the destination table (idempotent on re-runs).
"""

from __future__ import annotations

import os
import sqlite3

from tqdm import tqdm


oj = os.path.join


def run_transactions_use() -> None:
    """Join transactions_rfn + merchant_metadata_rfn → transactions_use."""
    path_db = oj("./database", "transactions.db")

    with sqlite3.connect(path_db) as db:
        db.row_factory = sqlite3.Row

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions_use (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                -- transaction fields
                transaction_id INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                date TEXT,
                amount REAL NOT NULL,
                transaction_type TEXT NOT NULL,
                currency TEXT,
                reference TEXT,
                -- merchant metadata fields
                merchant TEXT NOT NULL,
                category_main TEXT NOT NULL,
                category_second TEXT NOT NULL,
                city TEXT
            )
            """
        )

        rows = db.execute(
            """
            SELECT
                t.id              AS transaction_id,
                t.source_type,
                t.date,
                t.amount,
                t.transaction_type,
                t.currency,
                t.reference,
                m.matched_merchant  AS merchant,
                m.category_main,
                m.category_second,
                m.city
            FROM transactions_rfn t
            JOIN merchant_metadata_rfn m ON m.zkb_reference = t.reference
            WHERE t.enrichment_status = 'enriched'
              AND t.reference NOT IN (
                  SELECT reference FROM transactions_use
                  WHERE reference IS NOT NULL
              )
            ORDER BY t.date DESC
            """
        ).fetchall()

        for row in rows:
            db.execute(
                """
                INSERT INTO transactions_use (
                    transaction_id, source_type, date, amount, transaction_type,
                    currency, reference,
                    merchant, category_main, category_second, city
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["transaction_id"],
                    row["source_type"],
                    row["date"],
                    row["amount"],
                    row["transaction_type"],
                    row["currency"],
                    row["reference"],
                    row["merchant"],
                    row["category_main"],
                    row["category_second"],
                    row["city"],
                ),
            )

        db.commit()

    tqdm.write(f"transactions_use: {len(rows)} rows written")
