from __future__ import annotations

from swiss_exp_tracker.db.sql import agentic
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MetadataResult
from swiss_exp_tracker.pipeline_ingestion.db import get_connection

__all__ = ["MerchantResult", "MetadataResult"]


class MerchantResult:
    def __init__(self) -> None:
        pass

    def save_merchant_result(self, merchant_result: MetadataResult) -> None:
        """Persist one raw merchant enrichment result to the DB."""
        with get_connection() as db:
            agentic.create_merchant_metadata_raw_table(db)
            agentic.insert_merchant_metadata_raw(
                db,
                created_at=merchant_result.current_datetime.isoformat(),
                reference_id=merchant_result.reference_id,
                matched_merchant=merchant_result.matched_merchant,
                cache_hit=int(merchant_result.cache_hit),
                similarity=merchant_result.similarity,
                search_tool=(
                    merchant_result.search_tool.value
                    if merchant_result.search_tool
                    else None
                ),
                category_main=merchant_result.category_main,
                category_second=merchant_result.category_second,
                city=merchant_result.city,
            )
            db.commit()

    def mark_transaction_enriched(self, refined_id: int | None) -> None:
        """Update transactions_rfn.enrichment_status to 'enriched' for the given row id."""
        if refined_id is None:
            return
        with get_connection() as db:
            agentic.mark_transaction_enriched(db, refined_id=refined_id)
            db.commit()

    def mark_transaction_skipped(self, refined_id: int | None) -> None:
        """Set enrichment_status to 'skipped' for a row with no merchant to enrich."""
        if refined_id is None:
            return
        with get_connection() as db:
            agentic.mark_transaction_skipped(db, refined_id=refined_id)
            db.commit()

    def mark_transaction_needs_review(self, refined_id: int | None) -> None:
        """Set enrichment_status to 'needs_review' for a row that needs manual investigation."""
        if refined_id is None:
            return
        with get_connection() as db:
            agentic.mark_transaction_needs_review(db, refined_id=refined_id)
            db.commit()
