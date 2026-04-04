from __future__ import annotations

import re

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from agents import Runner
from agents import gen_trace_id
from agents import trace

from swiss_exp_tracker.pipeline_agentic.agents.agent_metadata import metadata_agent
from swiss_exp_tracker.pipeline_agentic.agents.agent_summary import summary_agent
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MERCHANT_BRANDS
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import (
    MERCHANT_COMPOUND_BRANDS,
)
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
            # 1. Get Merchant from booking text
            merchant = await self.extract_merchant(transaction)
            merchant.merchant = self._normalize_merchant(merchant.merchant)
            yield f"Got Merchant: {merchant}"

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

    def _normalize_merchant(self, merchant: str) -> str:
        normalized = merchant.lower()
        normalized = normalized.replace("ü", "u").replace("ä", "a").replace("ö", "o")

        # 1. Check compound/sub-brands first (specific patterns before generic ones)
        for pattern, canonical in MERCHANT_COMPOUND_BRANDS:
            if pattern in normalized:
                return canonical

        # 2. Check simple known brands
        for brand in MERCHANT_BRANDS:
            if brand in normalized:
                return brand

        # 3. Fallback: strip numbers, special chars, location suffixes
        name = re.sub(r"\d+", "", normalized)  # remove store IDs
        name = re.sub(r"[^a-z\s]", " ", name)  # remove special chars
        name = re.sub(r"\b(ag|sa|gmbh|ltd)\b", "", name)  # remove legal suffixes
        name = re.sub(r"\s+", " ", name).strip()

        # Take only first word as brand name
        return name if name else merchant

    async def extract_merchant(self, transaction: Transaction) -> MerchantExtractor:
        """Extract merchant name from booking text using regex"""
        text = transaction.booking_text.strip()
        is_person = self._contains_phone_number(text)

        if "Purchase" in text:
            # After the first comma
            match = re.search(r",\s*(.*)", text)
        else:
            # After the first colon
            match = re.search(r":\s*(.*)$", text)

        if match:
            return MerchantExtractor(
                merchant=match.group(1).strip(), is_person=is_person
            )

        # Fallback: return the full string if no match
        return MerchantExtractor(merchant=text, is_person=is_person)

    def _contains_phone_number(self, text: str) -> bool:
        """Detect phone-like sequences with at least 7 digits."""
        for candidate in re.findall(r"(?:\+|00)?\d[\d\s()./-]{5,}\d", text):
            digits = re.sub(r"\D", "", candidate)
            if len(digits) >= 7:
                return True
        return False

    def _detect_search_tool(self, result: Any) -> str:
        """Inspect RunResult.new_items to determine which search tool was called."""
        for item in getattr(result, "new_items", []):
            raw = getattr(item, "raw_item", None)
            if raw is None:
                continue
            name = str(getattr(raw, "name", "") or "").lower()
            tool_type = str(getattr(raw, "type", "") or "").lower()
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
