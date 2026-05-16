"""Top-level pipeline runner.

Runs the full data pipeline in sequence:
  1. Ingestion  — landing → raw → refined → postprocess
  2. Enrichment — agentic web search + metadata extraction
  3. Post-clean — manual corrections → merchant_metadata_rfn
  4. Final join — transactions_use (analysis-ready table)

Run from the project root:
    python -m swiss_exp_tracker.pipeline
"""

from __future__ import annotations

import asyncio
import logging

from swiss_exp_tracker.pipeline_agentic.clean_pipeline_output import run_post_clean
from swiss_exp_tracker.pipeline_agentic.pipeline import load_pending_transactions
from swiss_exp_tracker.pipeline_agentic.pipeline import run_all_transactions
from swiss_exp_tracker.pipeline_agentic.transactions_use import run_transactions_use
from swiss_exp_tracker.pipeline_ingestion.pipeline import run_ingestion


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    # ── Stage 1: Ingestion ────────────────────────────────────────────────────
    logger.info("=== Stage 1: Ingestion ===")
    run_ingestion()

    # ── Stage 2: Agentic enrichment ───────────────────────────────────────────
    logger.info("=== Stage 2: Agentic enrichment ===")
    transactions = load_pending_transactions()[:1000]
    if transactions:
        asyncio.run(run_all_transactions(transactions))
    else:
        logger.info("No pending transactions — skipping enrichment.")

    # ── Stage 3: Post-clean ───────────────────────────────────────────────────
    logger.info("=== Stage 3: Post-clean ===")
    run_post_clean()

    # ── Stage 4: Build final analysis table ───────────────────────────────────
    logger.info("=== Stage 4: Build transactions_use ===")
    run_transactions_use()

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
