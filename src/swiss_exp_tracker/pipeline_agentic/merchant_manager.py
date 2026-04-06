from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from agents import Runner
from agents import gen_trace_id
from agents import trace

from swiss_exp_tracker.pipeline_agentic.agents_.agent_metadata import metadata_agent
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
                search_tool = "none (person detected)"

            else:
                # 3. check if vector store already contains merchant
                cache_result = self.store.search(merchant.merchant)
                if cache_result:
                    cached_metadata, similarity = cache_result

                    result = MetadataResult(
                        current_datetime=datetime.now(),
                        zkb_reference=transaction.zkb_reference,
                        matched_merchant=cached_metadata.name,
                        cache_hit=True,
                        similarity=similarity,
                        search_tool=None,
                        category_main=cached_metadata.category_main.value,
                        category_second=cached_metadata.category_second,
                        city=cached_metadata.city,
                    )

                    self.result.save_merchant_result(result)
                    return

                # 4. Get summary of Merchant
                summary, search_tool = await self.get_merchant_summary(merchant)
                yield f"Got summary via {search_tool}, get merchant metadata"

                # 5. Get Merchant metadata
                merchant_metadata = await self.get_merchant_metadata(
                    merchant, transaction, summary
                )
                yield "Got Metadata"

            # 6. Save merchant metadata to store
            self.store.save(merchant.merchant, summary, merchant_metadata)
            yield f"Saved {merchant.merchant} to store"

            # 7. Save results to relational db
            result = MetadataResult(
                current_datetime=datetime.now(),
                zkb_reference=transaction.zkb_reference,
                matched_merchant=merchant_metadata.name,
                cache_hit=False,
                similarity=None,
                search_tool=search_tool,
                category_main=merchant_metadata.category_main.value,
                category_second=merchant_metadata.category_second,
                city=merchant_metadata.city,
            )

            self.result.save_merchant_result(result)

    def _detect_search_tool(self, result: Any) -> str:
        """Inspect RunResult.new_items to determine which search tool was called."""
        for item in getattr(result, "new_items", []):
            raw = getattr(item, "raw_item", None)
            if raw is None:
                continue
            name = str(getattr(raw, "name", "") or "").lower()
            tool_type = str(getattr(raw, "type", "") or "").lower()
            if name == "search_with_fallback":
                return "tavily/serpapi-router"
            if "tavily" in name or "tavily" in tool_type:
                return "tavily"
            if name == "serpapi_search":
                return "serpapi"
            if "web_search" in name or "web_search" in tool_type:
                return "websearch"
        return "unknown"

    async def get_merchant_summary(
        self, merchant: MerchantExtractor
    ) -> tuple[str, str]:
        """Get merchant summary. Returns (summary, search_tool_used)."""
        result = await Runner.run(summary_agent, merchant.model_dump_json())
        return result.final_output_as(str), self._detect_search_tool(result)

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

        result = await Runner.run(metadata_agent, agent_input.model_dump_json())
        return result.final_output_as(MerchantMetaData)
