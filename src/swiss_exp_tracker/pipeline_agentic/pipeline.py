from __future__ import annotations

import asyncio
import logging
import time

from datetime import datetime

from openai import APIError
from tqdm import tqdm

from swiss_exp_tracker.db.sql import agentic
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import (
    AllProvidersExhaustedError,
)
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import Transaction
from swiss_exp_tracker.pipeline_agentic.merchant_manager import MerchantManager
from swiss_exp_tracker.pipeline_ingestion.db import get_connection

logging.getLogger("httpx").setLevel(logging.WARNING)


def load_pending_transactions() -> list[Transaction]:
    """Step 1: Load transactions from database where enrichment is pending.

    Reads all rows from transactions_rfn table where enrichment_status = 'PENDING'
    and converts them to Transaction objects for processing.
    """
    pending_transactions: list[Transaction] = []

    with get_connection() as db:
        db.row_factory = None  # Reset row factory to get tuples
        rows = list(agentic.get_pending_transactions(db))

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


# Step 2: For each transaction, run the agentic pipeline and store results in database
async def run_all_transactions(
    transactions: list[Transaction], concurrency: int = 5
) -> None:
    """Process transactions concurrently.

    ``concurrency`` controls how many transactions run in parallel.
    Keep it low (3-8) to avoid hitting API rate limits on the web-search
    providers and the OpenAI API.
    """
    manager = MerchantManager()
    semaphore = asyncio.Semaphore(concurrency)
    merchant_locks: dict[str, asyncio.Lock] = {}
    t_start = time.perf_counter()
    # Shared flag: set to an error when all providers are exhausted so the
    # gather loop can stop cleanly without cancelling in-flight tasks.
    exhausted_exc: AllProvidersExhaustedError | None = None

    with tqdm(
        total=len(transactions), unit="tx", desc="Enriching transactions"
    ) as pbar:

        async def process_one(transaction: Transaction) -> None:
            nonlocal exhausted_exc
            if exhausted_exc is not None:
                return

            merchant_key = (
                (transaction.merchant or transaction.booking_text or "").strip().lower()
            )
            if merchant_key not in merchant_locks:
                merchant_locks[merchant_key] = asyncio.Lock()
            merchant_lock = merchant_locks[merchant_key]

            async with semaphore, merchant_lock:
                merchant_label = transaction.merchant or transaction.booking_text or "?"
                pbar.set_postfix_str(merchant_label[:40], refresh=True)
                try:
                    async for _step in manager.run(transaction):
                        pass  # steps are logged inside manager.run via print
                except AllProvidersExhaustedError as exc:
                    exhausted_exc = exc
                    tqdm.write(
                        f"\n[STOP] All web-search providers are exhausted and no pay-per-use keys are configured.\n"
                        f"{exc}\n"
                        "To resume enrichment:\n"
                        "  • Set BRAVE_SEARCH_API_KEY in .env for unlimited Brave Search pay-per-use.\n"
                        "  • Set EXA_API_KEY in .env for unlimited Exa pay-per-use.\n"
                        "  • Or wait for the monthly free-tier reset (Tavily/Brave/Scrape.do/Exa: 1,000 req/month each).\n"
                        "Pipeline stopped. No further transactions will be processed this run."
                    )
                    return
                except APIError as exc:
                    # Transient OpenAI errors are retried inside MerchantManager;
                    # if they still surface here, skip this transaction (it stays
                    # pending for a future run) rather than crash the whole batch.
                    tqdm.write(
                        f"[WARN] OpenAI API error ({type(exc).__name__}) for "
                        f"'{merchant_label[:40]}' — skipping; left pending for next run."
                    )
                pbar.update(1)

        await asyncio.gather(*(process_one(tx) for tx in transactions))

    if exhausted_exc is not None:
        return

    elapsed = time.perf_counter() - t_start
    tqdm.write(
        f"\nDone. {len(transactions)} transactions in {elapsed:.1f}s "
        f"({elapsed / len(transactions):.2f}s/tx avg)"
    )
