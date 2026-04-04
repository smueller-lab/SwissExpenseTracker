from __future__ import annotations

import importlib
import logging
import sqlite3
import sys

from datetime import date as date_type
from pathlib import Path
from typing import Any


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from swiss_exp_tracker.pipeline_ingestion.config import INGESTION_DB_PATH
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables


logger = logging.getLogger(__name__)

# Numeric-prefixed modules must be loaded via importlib
_mod_landing = importlib.import_module(
    "swiss_exp_tracker.pipeline_ingestion.stages.stage_01_landing"
)
_mod_raw = importlib.import_module(
    "swiss_exp_tracker.pipeline_ingestion.stages.stage_02_raw"
)
_mod_refined = importlib.import_module(
    "swiss_exp_tracker.pipeline_ingestion.stages.stage_03_refined"
)

run_landing = _mod_landing.run_landing
run_raw = _mod_raw.run_raw
run_refined = _mod_refined.run_refined


def _load_pending_transactions() -> list[Any]:
    """
    Query transactions_refined for rows with enrichment_status='pending'
    and convert them to pipeline_agentic Transaction objects.
    """
    from swiss_exp_tracker.pipeline_agentic.data_models.merchant import Transaction

    with sqlite3.connect(INGESTION_DB_PATH) as db:
        rows = db.execute(
            """
            SELECT id, date, amount, booking_text, reference
            FROM transactions_refined
            WHERE enrichment_status = 'pending'
            ORDER BY date ASC, id ASC
            """
        ).fetchall()

    transactions: list[Transaction] = []
    for _row_id, raw_date, amount, booking_text, reference in rows:
        date_obj: date_type | None = (
            date_type.fromisoformat(raw_date) if raw_date else None
        )
        tx = Transaction.model_construct(
            Date=date_obj,
            booking_text=booking_text or "",
            zkb_reference=reference,
            balance_chf=None,
            amount_chf=float(amount) if amount is not None else None,
        )
        transactions.append(tx)

    return transactions


def run_ingestion() -> dict[str, Any]:
    """Run landing → raw → refined in sequence and return stage-level stats."""
    create_all_tables()

    logger.info("[pipeline] Stage 1: landing")
    landing_result = run_landing()
    logger.info("           %s", landing_result)

    logger.info("[pipeline] Stage 2: raw")
    raw_result = run_raw()
    logger.info("           %s", raw_result)

    logger.info("[pipeline] Stage 3: refined")
    refined_result = run_refined()
    logger.info("           %s", refined_result)

    return {
        "landing": landing_result,
        "raw": raw_result,
        "refined": refined_result,
    }


async def run_enrichment() -> None:
    """Load pending refined rows and hand them off to pipeline_agentic for enrichment."""
    from swiss_exp_tracker.pipeline_agentic.pipeline import run_all_transactions

    transactions = _load_pending_transactions()
    logger.info(
        "[pipeline] Stage 4: enrichment — %d pending transaction(s)",
        len(transactions),
    )
    await run_all_transactions(transactions)


def run() -> None:
    """Full pipeline: ingestion (landing → raw → refined) then agentic enrichment."""
    run_ingestion()
    # asyncio.run(run_enrichment())


if __name__ == "__main__":
    run()
