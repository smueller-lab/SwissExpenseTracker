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

from swiss_exp_tracker.config import HOUSING_RENT_1
from swiss_exp_tracker.config import HOUSING_RENT_2
from swiss_exp_tracker.config import HOUSING_RENT_2_ROOMMATE_OFFSET
from swiss_exp_tracker.config import HOUSING_RENT_AMOUNTS_1
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategorySecond


# Merchant-name substrings + allowed amounts → (category_main, category_second)
AMOUNT_CORRECTIONS: list[tuple[list[str], list[float], tuple[str, str]]] = [
    (
        HOUSING_RENT_1,
        HOUSING_RENT_AMOUNTS_1,
        (CategoryMain.HOUSING.value, CategorySecond.HOUSING_RENT.value),
    ),
]


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
                category_second TEXT,
                city TEXT,
                balance_chf REAL
            )
            """
        )

        use_columns = {
            str(col[1])
            for col in db.execute("PRAGMA table_info(transactions_use)").fetchall()
        }
        if "balance_chf" not in use_columns:
            db.execute("ALTER TABLE transactions_use ADD COLUMN balance_chf REAL")

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
                m.city,
                t.balance_chf
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
                    merchant, category_main, category_second, city, balance_chf
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    row["balance_chf"],
                ),
            )

        db.commit()

    tqdm.write(f"transactions_use: {len(rows)} rows written")

    _backfill_balance_chf_use()
    _sync_categories_from_rfn()


def _sync_categories_from_rfn() -> None:
    """Update existing transactions_use rows whose rfn categories changed."""
    path_db = oj("./database", "transactions.db")

    with sqlite3.connect(path_db) as db:
        db.row_factory = sqlite3.Row

        rows = db.execute(
            """
            SELECT tu.id, m.category_main, m.category_second, m.city
            FROM transactions_use tu
            JOIN merchant_metadata_rfn m ON m.zkb_reference = tu.reference
            WHERE tu.category_main  != m.category_main
               OR COALESCE(tu.category_second, '') != COALESCE(m.category_second, '')
               OR COALESCE(tu.city, '')            != COALESCE(m.city, '')
            """
        ).fetchall()

        for row in rows:
            db.execute(
                """
                UPDATE transactions_use
                SET category_main = ?, category_second = ?, city = ?
                WHERE id = ?
                """,
                (row["category_main"], row["category_second"], row["city"], row["id"]),
            )

        db.commit()

    tqdm.write(f"transactions_use: {len(rows)} rows synced from rfn")

    _apply_amount_corrections()


def _apply_amount_corrections() -> None:
    """Apply merchant+amount based category overrides to transactions_use."""
    path_db = oj("./database", "transactions.db")

    with sqlite3.connect(path_db) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT id, merchant, amount, category_main, category_second FROM transactions_use"
        ).fetchall()

        rows_updated = 0
        for row in rows:
            merchant_key = (row["merchant"] or "").lower()
            amount = row["amount"]

            for patterns, amounts, (cat_main, cat_second) in AMOUNT_CORRECTIONS:
                if (
                    any(p.lower() in merchant_key for p in patterns)
                    and amount in amounts
                ):
                    if (
                        row["category_main"] == cat_main
                        and row["category_second"] == cat_second
                    ):
                        break
                    db.execute(
                        "UPDATE transactions_use SET category_main = ?, category_second = ? WHERE id = ?",
                        (cat_main, cat_second, row["id"]),
                    )
                    rows_updated += 1
                    break

        db.commit()

    tqdm.write(f"transactions_use: {rows_updated} rows corrected by amount")

    _apply_shared_housing_roommate_offset()


def _apply_shared_housing_roommate_offset() -> None:
    """Subtract roommate's share from Shared housing rent and remove the matching income row.

    Idempotent: resets Shared housing amounts from transactions_rfn before applying, so
    repeated runs always produce the same result even if income rows were
    re-inserted by a previous run_transactions_use() call.
    """
    path_db = oj("./database", "transactions.db")

    like_clauses = " OR ".join(
        f"lower(tu.merchant) LIKE '%{p.lower()}%'" for p in HOUSING_RENT_2
    )

    with sqlite3.connect(path_db) as db:
        db.row_factory = sqlite3.Row

        # Reset Shared housing amounts to the original source value (transactions_rfn)
        db.execute(
            f"""
            UPDATE transactions_use
            SET amount = (
                SELECT t.amount
                FROM transactions_rfn t
                WHERE t.reference = transactions_use.reference
            )
            WHERE {like_clauses.replace("tu.", "")}
            """
        )

        shared_housing_rows = db.execute(
            f"""
            SELECT tu.id, tu.date, tu.amount
            FROM transactions_use tu
            WHERE ({like_clauses})
              AND tu.category_main = 'Housing'
              AND tu.category_second = 'Rent'
            ORDER BY tu.date
            """
        ).fetchall()

        # First Shared housing row per month
        shared_housing_by_month: dict[str, tuple[int, float]] = {}
        for row in shared_housing_rows:
            month_key = row["date"][:7]
            if month_key not in shared_housing_by_month:
                shared_housing_by_month[month_key] = (
                    int(row["id"]),
                    float(row["amount"]),
                )

        income_rows = db.execute(
            "SELECT id, date FROM transactions_use WHERE amount = ? AND transaction_type = 'INCOME'",
            (HOUSING_RENT_2_ROOMMATE_OFFSET,),
        ).fetchall()

        rows_adjusted = 0
        ids_to_delete: list[int] = []
        for income in income_rows:
            month_key = income["date"][:7]
            if month_key in shared_housing_by_month:
                shared_housing_id, shared_housing_amount = shared_housing_by_month[
                    month_key
                ]
                db.execute(
                    "UPDATE transactions_use SET amount = ? WHERE id = ?",
                    (
                        shared_housing_amount - HOUSING_RENT_2_ROOMMATE_OFFSET,
                        shared_housing_id,
                    ),
                )
                ids_to_delete.append(income["id"])
                rows_adjusted += 1

        for income_id in ids_to_delete:
            db.execute("DELETE FROM transactions_use WHERE id = ?", (income_id,))

        db.commit()

    tqdm.write(
        f"transactions_use: {rows_adjusted} Shared housing roommate offsets applied"
    )


def _backfill_balance_chf_use() -> None:
    """Propagate balance_chf from transactions_rfn into existing transactions_use rows."""
    path_db = oj("./database", "transactions.db")

    with sqlite3.connect(path_db) as db:
        db.execute(
            """
            UPDATE transactions_use
            SET balance_chf = (
                SELECT t.balance_chf
                FROM transactions_rfn t
                WHERE t.reference = transactions_use.reference
            )
            WHERE balance_chf IS NULL
            """
        )
        db.commit()

    tqdm.write("transactions_use: balance_chf backfilled from transactions_rfn")
