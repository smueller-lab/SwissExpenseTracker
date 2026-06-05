from __future__ import annotations

import asyncio
import time

from collections.abc import AsyncIterator
from datetime import datetime

from agents import Runner
from agents.exceptions import MaxTurnsExceeded
from agents.exceptions import OutputGuardrailTripwireTriggered
from tqdm import tqdm

from swiss_exp_tracker.db.sql import groceries
from swiss_exp_tracker.pipeline_agentic.agents_.agent_grocery import grocery_agent
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryData
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryDetail
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryResult
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryRow
from swiss_exp_tracker.pipeline_agentic.grocery_result import GroceryResult
from swiss_exp_tracker.pipeline_agentic.grocery_store import GroceryStore
from swiss_exp_tracker.pipeline_ingestion.db import get_connection


def load_pending_groceries() -> list[GroceryRow]:
    with get_connection() as db:
        rows = list(groceries.get_pending_groceries(db))

    return [
        GroceryRow(
            rfn_id=int(row[0]),
            article=str(row[1]),
            article_normalized=str(row[2]),
            location=str(row[3]),
        )
        for row in rows
    ]


class GroceryManager:
    def __init__(self) -> None:
        self.store = GroceryStore()
        self.result = GroceryResult()

    async def run(self, row: GroceryRow) -> AsyncIterator[str]:
        cache_result = self.store.search(row.article_normalized)

        if cache_result:
            cached_category, similarity = cache_result
            result = GroceryCategoryResult(
                current_datetime=datetime.now(),
                rfn_id=row.rfn_id,
                article=row.article,
                matched_article=cached_category.article,
                cache_hit=True,
                similarity=similarity,
                category_main=cached_category.category_main,
                category_detail=cached_category.category_detail,
            )
            self.result.save_grocery_result(result)
            self.result.mark_grocery_enriched(row.rfn_id)
            return

        category = await self._categorise(row)
        yield f"Categorised '{row.article}' → {category.category_main} / {category.category_detail}"

        self.store.save(row.article_normalized, category)
        yield f"Saved '{row.article_normalized}' to grocery store"

        result = GroceryCategoryResult(
            current_datetime=datetime.now(),
            rfn_id=row.rfn_id,
            article=row.article,
            matched_article=row.article_normalized,
            cache_hit=False,
            similarity=None,
            category_main=category.category_main,
            category_detail=category.category_detail,
        )
        self.result.save_grocery_result(result)
        self.result.mark_grocery_enriched(row.rfn_id)

    async def _categorise(self, row: GroceryRow) -> GroceryCategoryData:
        agent_input = {
            "article_normalized": row.article_normalized,
            "location": row.location,
        }
        import json

        payload = json.dumps(agent_input, ensure_ascii=False)

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                result = await Runner.run(grocery_agent, payload, max_turns=5)
                return result.final_output_as(GroceryCategoryData)
            except OutputGuardrailTripwireTriggered:
                tqdm.write(
                    f"[WARN] Guardrail triggered for '{row.article}' "
                    f"(attempt {attempt}/{max_retries}) — retrying"
                )
            except MaxTurnsExceeded:
                tqdm.write(
                    f"[WARN] MaxTurnsExceeded for '{row.article}' "
                    f"(attempt {attempt}/{max_retries}) — retrying"
                )

        tqdm.write(
            f"[WARN] All {max_retries} attempts failed for '{row.article}' — using fallback"
        )
        return GroceryCategoryData(
            article=row.article_normalized,
            category_main=GroceryCategoryMain.OTHER,
            category_detail=GroceryCategoryDetail.UNKNOWN,
        )


async def run_all_groceries(rows: list[GroceryRow], concurrency: int = 10) -> None:
    if not rows:
        tqdm.write("No pending groceries to categorise.")
        return

    manager = GroceryManager()
    semaphore = asyncio.Semaphore(concurrency)
    article_locks: dict[str, asyncio.Lock] = {}
    t_start = time.perf_counter()

    with tqdm(total=len(rows), unit="item", desc="Categorising groceries") as pbar:

        async def process_one(row: GroceryRow) -> None:
            key = row.article_normalized.lower()
            if key not in article_locks:
                article_locks[key] = asyncio.Lock()

            async with semaphore, article_locks[key]:
                pbar.set_postfix_str(row.article[:40], refresh=True)
                async for _step in manager.run(row):
                    pass
                pbar.update(1)

        await asyncio.gather(*(process_one(row) for row in rows))

    elapsed = time.perf_counter() - t_start
    tqdm.write(
        f"\nDone. {len(rows)} groceries in {elapsed:.1f}s "
        f"({elapsed / len(rows):.2f}s/item avg)"
    )
