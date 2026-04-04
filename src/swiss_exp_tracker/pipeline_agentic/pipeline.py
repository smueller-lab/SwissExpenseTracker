from __future__ import annotations

from typing import Any

from dotenv import load_dotenv

from swiss_exp_tracker.pipeline_agentic.data_models.merchant import Transaction
from swiss_exp_tracker.pipeline_agentic.merchant_manager import MerchantManager


load_dotenv(override=True)


# TODO: Redo agentic pipeline with pipeline_ingestion


async def run_all_transactions(transactions: list[Transaction]) -> None:
    manager = MerchantManager()
    results: list[dict[Any, Any]] = []

    for _, transaction in enumerate(transactions, 1):
        async for step in manager.run(transaction):
            if isinstance(step, dict):
                results.append(step)


# all_transactions = load_transactions(pth)
# transactions = all_transactions[8:10]

# asyncio.run(run_all_transactions(transactions))
