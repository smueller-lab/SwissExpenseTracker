from __future__ import annotations

import asyncio

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from agents import Agent
from agents import Runner
from agents import RunResult
from agents import gen_trace_id
from agents import trace
from agents.exceptions import MaxTurnsExceeded
from agents.exceptions import OutputGuardrailTripwireTriggered
from openai import APIConnectionError
from openai import InternalServerError
from openai import RateLimitError
from tqdm import tqdm

from swiss_exp_tracker.pipeline_agentic.agents_.agent_metadata import metadata_agent
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import (
    AllProvidersExhaustedError,
)
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import SearchToolResult
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import WebSearchTool
from swiss_exp_tracker.pipeline_agentic.agents_.agent_summary import summary_agent
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategorySecond
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MerchantExtractor
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MerchantMetaData
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MerchantMetaInput
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import Transaction
from swiss_exp_tracker.pipeline_agentic.merchant_result import MerchantResult
from swiss_exp_tracker.pipeline_agentic.merchant_result import MetadataResult
from swiss_exp_tracker.pipeline_agentic.merchant_store import MerchantStore

# Transient OpenAI failures worth retrying: timeouts (APITimeoutError subclasses
# APIConnectionError), dropped connections, rate limits and 5xx responses.
_RETRYABLE_OPENAI_ERRORS = (
    APIConnectionError,
    RateLimitError,
    InternalServerError,
)
_OPENAI_RETRY_WAIT_S = 3.0
_OPENAI_MAX_RETRIES = 5


async def _run_agent_with_retry(
    agent: Agent[Any], agent_input: str, max_turns: int
) -> RunResult:
    """Run an agent, retrying transient OpenAI errors after a 3s wait; re-raises if all fail."""
    for attempt in range(1, _OPENAI_MAX_RETRIES + 1):
        try:
            return await Runner.run(agent, agent_input, max_turns=max_turns)
        except _RETRYABLE_OPENAI_ERRORS as exc:
            if attempt == _OPENAI_MAX_RETRIES:
                raise
            tqdm.write(
                f"[WARN] OpenAI API error ({type(exc).__name__}) on attempt "
                f"{attempt}/{_OPENAI_MAX_RETRIES} — waiting "
                f"{_OPENAI_RETRY_WAIT_S:.0f}s and retrying"
            )
            await asyncio.sleep(_OPENAI_RETRY_WAIT_S)
    raise RuntimeError("unreachable: retry loop exited without returning")


class MerchantManager:
    def __init__(self) -> None:
        self.store = MerchantStore()
        self.result = MerchantResult()

    # research manager
    async def run(self, transaction: Transaction) -> AsyncIterator[str]:
        """
        Get Metadata from Merchant booking text and yield results of steps.
        """
        trace_id = gen_trace_id()
        with trace("Research trace", trace_id=trace_id):
            # 1. Use merchant and person flag from refined DB row
            merchant_name = (transaction.merchant or "").strip()
            if not merchant_name:
                # No merchant to research (e.g. card-bill settlement rows) —
                # mark skipped so it leaves the pending pool permanently.
                self.result.mark_transaction_skipped(transaction.refined_id)
                return

            merchant = MerchantExtractor(
                merchant=merchant_name,
                is_person=bool(transaction.is_person),
            )

            # 2. Check if merchant is a person and set predefined metadata
            if merchant.is_person:
                merchant_metadata = MerchantMetaData(
                    name=merchant.merchant,
                    category_main=CategoryMain.FRIEND,
                    category_second=CategorySecond.FRIEND_SUPPORT_PAYMENT,
                    city="",
                )
                summary = f"{merchant.merchant} is a person, categorized as FRIEND_SUPPORT_PAYMENT"
                search_result = SearchToolResult(
                    summary=summary, tool_used=WebSearchTool.PERSON
                )
                result: SearchToolResult | MetadataResult = search_result

            else:
                # 3. check if vector store already contains merchant
                cache_result = self.store.search(merchant.merchant)
                if cache_result:
                    cached_metadata, similarity = cache_result

                    result = MetadataResult(
                        current_datetime=datetime.now(),
                        reference_id=transaction.reference_id,
                        matched_merchant=cached_metadata.name,
                        cache_hit=True,
                        similarity=similarity,
                        search_tool=None,
                        category_main=(
                            cached_metadata.category_main.value
                            if cached_metadata.category_main is not None
                            else None
                        ),
                        category_second=cached_metadata.category_second,
                        city=cached_metadata.city,
                    )

                    self.result.save_merchant_result(result)
                    self.result.mark_transaction_enriched(transaction.refined_id)
                    return

                # 4. Get summary of Merchant
                search_result = await self.get_merchant_summary(merchant)
                result = search_result
                yield f"Got summary via {search_result.tool_used}, get merchant metadata"

                # 5. Get Merchant metadata
                merchant_metadata = await self.get_merchant_metadata(
                    merchant, transaction, search_result.summary
                )
                yield "Got Metadata"

            # 6. Save merchant metadata to store (skip if categories are unknown)
            if merchant_metadata.category_main is not None:
                self.store.save(
                    merchant.merchant, search_result.summary, merchant_metadata
                )
                yield f"Saved {merchant.merchant} to store"
            else:
                yield f"Skipped store save for '{merchant.merchant}' (no category)"

            # 7. Save results to relational db
            result = MetadataResult(
                current_datetime=datetime.now(),
                reference_id=transaction.reference_id,
                matched_merchant=merchant_metadata.name,
                cache_hit=False,
                similarity=None,
                search_tool=search_result.tool_used,
                category_main=(
                    merchant_metadata.category_main.value
                    if merchant_metadata.category_main is not None
                    else None
                ),
                category_second=merchant_metadata.category_second,
                city=merchant_metadata.city,
            )

            self.result.save_merchant_result(result)

            # 8. Update pending transaction as enriched in DB
            self.result.mark_transaction_enriched(transaction.refined_id)

    async def get_merchant_summary(
        self, merchant: MerchantExtractor
    ) -> SearchToolResult:
        """Get merchant summary. Returns (summary, search_tool_used)."""
        try:
            run_result = await _run_agent_with_retry(
                summary_agent, merchant.model_dump_json(), max_turns=10
            )
            search_result = run_result.final_output_as(SearchToolResult)
            if search_result.tool_used == WebSearchTool.NO_WEBSEARCH:
                raise AllProvidersExhaustedError(search_result.summary)
            return search_result
        except MaxTurnsExceeded:
            tqdm.write(
                f"[WARN] MaxTurnsExceeded for summary of '{merchant.merchant}' — returning fallback"
            )
            return SearchToolResult(
                summary=f"Summary unavailable for '{merchant.merchant}' (max turns exceeded).",
                tool_used=WebSearchTool.NO_WEBSEARCH,
            )

    async def get_merchant_metadata(
        self, merchant: MerchantExtractor, transaction: Transaction, summary: str
    ) -> MerchantMetaData:
        """Get merchant metadata based on merchant name and merchant summary"""
        booking_text = transaction.booking_text

        agent_input = MerchantMetaInput(
            merchant=merchant.merchant,
            booking_text=booking_text,
            merchant_summary=summary,
        )

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                result = await _run_agent_with_retry(
                    metadata_agent, agent_input.model_dump_json(), max_turns=10
                )
                return result.final_output_as(MerchantMetaData)
            except OutputGuardrailTripwireTriggered:
                tqdm.write(
                    f"[WARN] Guardrail triggered for '{merchant.merchant}' "
                    f"(attempt {attempt}/{max_retries}) — retrying"
                )
            except MaxTurnsExceeded:
                tqdm.write(
                    f"[WARN] MaxTurnsExceeded for metadata of '{merchant.merchant}' "
                    f"(attempt {attempt}/{max_retries}) — retrying"
                )

        tqdm.write(
            f"[WARN] All {max_retries} attempts failed for '{merchant.merchant}' — returning fallback"
        )
        metadata = MerchantMetaData(
            name=merchant.merchant,
            category_main=None,
            category_second=None,
            city=None,
        )
        tqdm.write(
            f"[WARN] '{merchant.merchant}' — no category found after {max_retries} retries, "
            "saved with category_main=None"
        )
        return metadata
