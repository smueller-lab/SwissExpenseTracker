from __future__ import annotations

import json
import re

from datetime import datetime
from typing import Any

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MERCHANT_BRANDS
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import (
    MERCHANT_COMPOUND_BRANDS,
)
from swiss_exp_tracker.pipeline_ingestion.adapters.coercion import parse_swiss_float
from swiss_exp_tracker.pipeline_ingestion.adapters.generic_adapter import to_unified
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import (
    SUPPORTED_SOURCES,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import get_profile
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.tables import RawRow
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import Currency
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    EnrichmentStatus,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.source_hooks import (
    DebitEBankingContext,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.source_hooks import (
    clean_zkb_ebanking,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.source_hooks import (
    get_revolut_chf_map,
)


def _is_person_transaction(text: str) -> bool:
    """Detect phone-like sequences with at least 7 digits and if it's a TWINT transaction"""
    for candidate in re.findall(r"(?:\+|00)?\d[\d\s()./-]{5,}\d", text):
        digits = re.sub(r"\D", "", candidate)
        if len(digits) >= 7 and "TWINT" in text:
            return True
    return False


def _normalize_merchant(merchant: str, booking_text: str) -> tuple[str, bool]:
    """Normalize merchant names by lowercasing, replacing common umlauts, and applying known brand patterns."""
    is_person = _is_person_transaction(booking_text)
    normalized = merchant.lower()
    normalized = normalized.replace("ü", "u").replace("ä", "a").replace("ö", "o")

    for pattern, canonical in MERCHANT_COMPOUND_BRANDS:
        if pattern in normalized:
            return canonical, is_person

    for brand in MERCHANT_BRANDS:
        if brand in normalized:
            return brand, is_person

    name = re.sub(r"\d+", "", normalized)
    name = re.sub(r"[^a-z\s]", " ", name)
    name = re.sub(r"\b(ag|sa|gmbh|ltd)\b", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return (name if name else merchant), is_person


def _booking_text_split(text: str) -> str:
    """Split booking text based on known patterns."""
    if "TWINT" in text:
        return text.split(":", 1)[-1]
    if "ZKB Visa Debit" in text:
        return text.split(",", 1)[-1]
    if "Debit eBanking" in text:
        return text.split(":", 1)[-1]
    if "ZKB Mastro card" in text:
        return text.split(",", 1)[-1]
    return text.split(":", 1)[-1]


def _extract_merchant_normalized(booking_text: str | None) -> tuple[str | None, bool]:
    """Extract and normalize merchant name from booking text; return (name, is_person)."""
    if not booking_text:
        return None, False

    split_text = _booking_text_split(booking_text).strip()
    if not split_text:
        return None, _is_person_transaction(booking_text)

    return _normalize_merchant(split_text, booking_text)


def _as_iso_datetime(value: datetime | None) -> str | None:
    """Return ISO string for a datetime, or None if value is None."""
    return value.isoformat() if value is not None else None


def _rows_from_query_result(rows: list[Any]) -> list[RawRow]:
    """Convert raw SQL row tuples into RawRow models."""
    return [
        RawRow(
            raw_id=int(row[0]),
            landing_id=int(row[1]),
            file_id=int(row[2]),
            source_type=SourceType(row[3]),
            raw_json=str(row[4]),
            source_file=str(row[5]),
        )
        for row in rows
    ]


def _load_unprocessed_raw_rows(source_type: SourceType) -> list[RawRow]:
    """Return all unprocessed raw rows for source_type, ordered by id."""
    with get_connection() as db:
        rows = list(
            transactions.get_unprocessed_raw_rows(db, source_type=source_type.value)
        )
    return _rows_from_query_result(rows)


def _load_all_raw_rows(source_type: SourceType) -> list[RawRow]:
    """Return all raw rows for source_type regardless of processed state, ordered by id."""
    with get_connection() as db:
        rows = list(transactions.get_all_raw_rows(db, source_type=source_type.value))
    return _rows_from_query_result(rows)


def process_refined_source(source_type: SourceType) -> dict[str, int]:
    """Transform unprocessed raw rows of a source into refined transactions.

    High-level flow:
    1. Ensure schema exists and load all unprocessed raw rows for the source.
    2. Precompute source-specific context (e.g. Revolut CHF conversion map).
    3. Parse each raw row via the generic profile-driven adapter and apply source hooks.
    4. Skip rows that should not become refined records (duplicates, exchange
       legs, topups, helper parent rows) while still marking them processed.
    5. Insert normalized rows into `transactions_rfn` and mark raw rows as
       processed.
    6. Promote ingested file status to `refined` when all file rows are fully
       processed.

    Returns counters for visibility into refinement throughput.
    """
    create_all_tables()

    rows = _load_unprocessed_raw_rows(source_type)
    profile = get_profile(source_type)

    # Precompute Revolut CHF conversion map before the main loop. Built from full
    # history (not just this batch's unprocessed rows) — the exchange event that
    # established a currency's rate may already be processed from an earlier run.
    revolut_chf_map: dict[int, float | None] = {}
    if source_type == SourceType.REVOLUT:
        revolut_chf_map = get_revolut_chf_map(_load_all_raw_rows(source_type), profile)

    rows_processed = 0
    records_inserted = 0
    duplicates_found = 0
    pending_skipped = 0
    processed_file_ids: set[int] = set()
    zkb_ebanking_context = DebitEBankingContext()

    with get_connection() as db:

        def mark_processed(raw_id: int, file_id: int) -> None:
            """Mark a raw row as processed and update local bookkeeping counters."""
            nonlocal rows_processed
            transactions.mark_raw_processed(db, raw_id=raw_id)
            processed_file_ids.add(file_id)
            rows_processed += 1

        for row in rows:
            payload: dict[str, object] = json.loads(row.raw_json)

            # Step 3a: ZKB debit eBanking parent/detail stitching.
            if source_type == SourceType.ZKB_DEBIT:
                cleaned = clean_zkb_ebanking(payload, zkb_ebanking_context, profile)
                if cleaned is None:
                    mark_processed(row.raw_id, row.file_id)
                    continue
                payload = cleaned

            # Extract running balance for debit-account sources (None elsewhere).
            balance_chf: float | None = None
            if profile.balance_column is not None:
                balance_chf = parse_swiss_float(payload.get(profile.balance_column))

            # Step 3b: Revolut exchange-leg and topup filtering.
            revolut_chf_signed: float | None = None
            if source_type == SourceType.REVOLUT:
                chf_candidate = revolut_chf_map.get(row.raw_id)
                if chf_candidate is None:
                    # Exchange legs are pre-marked as None; skip from refined output.
                    mark_processed(row.raw_id, row.file_id)
                    continue

                if str(payload.get("Type", "")).lower() == "topup":
                    # Topups are balance movements, not spending/income records.
                    mark_processed(row.raw_id, row.file_id)
                    continue

                revolut_chf_signed = chf_candidate

            # Step 3c: convert to unified transaction via the generic profile adapter.
            unified = to_unified(payload, profile, row.source_file)

            # Validate Revolut currency after exchange/topup skips — raises ValueError
            # for unrecognised currency strings (e.g. "XYZ"), preserving old behaviour.
            if (
                source_type == SourceType.REVOLUT
                and profile.currency.column is not None
            ):
                Currency(str(payload.get(profile.currency.column) or ""))

            if source_type == SourceType.REVOLUT and revolut_chf_signed is not None:
                # Override amount and direction using the precomputed signed CHF value.
                amount = abs(revolut_chf_signed)
                transaction_type = (
                    TransactionType.EXPENSE
                    if revolut_chf_signed < 0
                    else TransactionType.INCOME
                ).value
                currency = Currency.CHF.value
            else:
                amount = unified.amount
                transaction_type = unified.transaction_type.value
                currency = unified.currency.value

            # Step 3d: normalise values, check pending/duplicate, then insert.
            amount = round(amount, 2)
            date_iso = _as_iso_datetime(unified.date)

            # A unified transaction with no date is pending (not yet confirmed by the
            # bank). Drop it and let the confirmed row arrive in a later export.
            if date_iso is None:
                pending_skipped += 1
                mark_processed(row.raw_id, row.file_id)
                continue

            reference = unified.reference_id
            if source_type == SourceType.REVOLUT:
                # Revolut rows have no stable reference (NOID-<uuid> is random per
                # parse), so overlapping exports need a date+amount match instead.
                is_duplicate = bool(
                    transactions.check_duplicate_rfn_revolut(
                        db, date=date_iso, amount=amount
                    )
                )
            else:
                is_duplicate = bool(
                    transactions.check_duplicate_rfn(
                        db, reference=reference.strip(), date=date_iso, amount=amount
                    )
                )
            if is_duplicate:
                # Duplicate raw rows are still marked processed so the source
                # file can eventually advance to the next pipeline stage.
                duplicates_found += 1
                mark_processed(row.raw_id, row.file_id)
                continue

            merchant_normalized, is_person = _extract_merchant_normalized(
                unified.booking_text
            )

            enrichment_status = EnrichmentStatus.PENDING.value

            transactions.insert_transactions_rfn(
                db,
                raw_id=row.raw_id,
                source_type=SourceType(unified.source).value,
                date=date_iso,
                amount=amount,
                transaction_type=transaction_type,
                booking_text=unified.booking_text,
                merchant_normalized=merchant_normalized,
                is_person=int(is_person),
                currency=currency,
                reference=reference,
                enrichment_status=enrichment_status,
                created_at=datetime.now().isoformat(),
                balance_chf=balance_chf,
            )

            mark_processed(row.raw_id, row.file_id)
            records_inserted += 1

        # Step 4: close ingested files whose raw rows are now fully processed.
        for file_id in processed_file_ids:
            count = transactions.count_unprocessed_raw_for_file(db, file_id=file_id)
            if count:
                continue
            transactions.set_ingested_file_status(db, status="refined", file_id=file_id)

    return {
        "rows_found": len(rows),
        "rows_processed": rows_processed,
        "records_inserted": records_inserted,
        "duplicates_found": duplicates_found,
        "pending_skipped": pending_skipped,
    }


def run_refined() -> dict[str, dict[str, int]]:
    """Run refined stage for all known source types."""
    results: dict[str, dict[str, int]] = {}

    for source_type in SUPPORTED_SOURCES:
        result = process_refined_source(source_type)
        results[source_type.value] = result

    return results
