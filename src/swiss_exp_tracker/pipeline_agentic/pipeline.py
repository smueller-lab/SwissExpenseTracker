from __future__ import annotations

import asyncio

from datetime import datetime
from typing import Any

from tqdm import tqdm

from swiss_exp_tracker.pipeline_agentic.data_models.merchant import Transaction
from swiss_exp_tracker.pipeline_agentic.merchant_manager import MerchantManager
from swiss_exp_tracker.pipeline_ingestion.db import get_connection


def load_pending_transactions() -> list[Transaction]:
    """Step 1: Load transactions from database where enrichment is pending.

    Reads all rows from transactions_refined table where enrichment_status = 'PENDING'
    and converts them to Transaction objects for processing.
    """
    pending_transactions: list[Transaction] = []

    with get_connection() as db:
        db.row_factory = None  # Reset row factory to get tuples
        rows = db.execute(
            """
            SELECT 
                id,
                date,
                merchant_normalized,
                booking_text,
                reference,
                amount,
                is_person
            FROM transactions_refined
            WHERE enrichment_status = 'pending'
            ORDER BY created_at ASC
            """
        ).fetchall()

    for row in rows:
        (
            refined_id,
            date_str,
            merchant,
            booking_text,
            reference,
            amount,
            is_person,
        ) = row

        # Parse date if it exists
        transaction_date = None
        if date_str:
            transaction_date = datetime.fromisoformat(date_str).date()

        # Create Transaction object using model_validate for flexible input
        transaction = Transaction.model_validate(
            {
                "refined_id": refined_id,
                "Date": transaction_date,
                "merchant": merchant,
                "Booking text": booking_text,
                "ZKB reference": reference,
                "Balance CHF": None,
                "amount_chf": amount,
                "is_person": bool(is_person),
            }
        )

        pending_transactions.append(transaction)

    return pending_transactions


# Step 1: Load unprocessed transactions from database and convert to Transaction objects
transactions = load_pending_transactions()

# For testing, process only a subset of transactions
transactions = transactions[:1000].copy()


# Step 2: For each transaction, run the agentic pipeline and store results in database
async def run_all_transactions(transactions: list[Transaction]) -> None:
    manager = MerchantManager()
    results: list[dict[Any, Any]] = []

    with tqdm(
        total=len(transactions), unit="tx", desc="Enriching transactions"
    ) as pbar:
        for transaction in transactions:
            merchant_label = transaction.merchant or transaction.booking_text or "?"
            pbar.set_postfix_str(merchant_label[:40], refresh=True)
            async for step in manager.run(transaction):
                if isinstance(step, dict):
                    results.append(step)
            pbar.update(1)


asyncio.run(run_all_transactions(transactions))
