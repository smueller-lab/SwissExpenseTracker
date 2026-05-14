from __future__ import annotations

import json
import re
import sqlite3

from collections import defaultdict
from datetime import datetime
from typing import cast

from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MERCHANT_BRANDS
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import (
    MERCHANT_COMPOUND_BRANDS,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import (
    SOURCE_MODEL_MAP,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import (
    get_source_adapter_map,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.tables import RawRow
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import Currency
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    EnrichmentStatus,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    RevolutTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import ZKBTransaction
from swiss_exp_tracker.pipeline_ingestion.data_models.transformation import (
    DebitEBankingContext,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transformation import (
    RevolutExchangeEvent,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transformation import (
    RevolutExchangePending,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transformation import (
    RevolutRatePoint,
)
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection


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
    if not booking_text:
        return None, False

    split_text = _booking_text_split(booking_text).strip()
    if not split_text:
        return None, _is_person_transaction(booking_text)

    return _normalize_merchant(split_text, booking_text)


def _as_iso_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _load_unprocessed_raw_rows(source_type: SourceType) -> list[RawRow]:
    query = """
		SELECT tr.id, tr.landing_id, tl.file_id, tr.source_type, tr.raw_json, tr.source_file
		FROM transactions_raw tr
		JOIN transactions_lnd tl ON tl.id = tr.landing_id
		WHERE tr.processed = 0
		  AND tr.source_type = ?
		ORDER BY tr.id
	"""

    with get_connection() as db:
        rows = db.execute(query, (source_type.value,)).fetchall()

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


def _resolve_revolut_chf_amounts(
    rows: list[tuple[RawRow, RevolutTransaction]],
) -> dict[int, float | None]:
    """Resolve CHF-equivalent signed amounts for Revolut raw rows.

    The Revolut export can contain currency exchange rows as two linked entries:
    one negative leg in the source currency and one positive leg in the target
    currency. Those exchange rows should not become refined transactions, but
    they are used to build a simple historical CHF conversion timeline.

    Processing steps:
    1. Identify paired exchange rows and exclude them from refined output.
    2. Derive a running CHF rate for each target currency from exchange events.
    3. Convert non-exchange Revolut rows into signed CHF amounts using the most
       recent known rate at the transaction timestamp.

    Returns a mapping from `transactions_raw.id` to a signed CHF amount. Rows
    that represent exchange legs are mapped to `None` so the caller can skip
    them during refined insertion.
    """
    exchange_pending: dict[tuple[str, str], RevolutExchangePending] = {}
    exchange_row_ids: set[int] = set()
    exchange_events: list[RevolutExchangeEvent] = []

    # Step 1: pair matching exchange legs and collect events for later CHF-rate
    # reconstruction. Unmatched exchange rows are also excluded from output.
    for raw_row, tx in rows:
        if tx.Type.lower() != "exchange":
            continue

        pair_key = (tx.Description, tx.StartedDate.isoformat())
        if pair_key in exchange_pending:
            other_pending = exchange_pending.pop(pair_key)
            exchange_row_ids.add(raw_row.raw_id)
            exchange_row_ids.add(other_pending.raw_id)

            if tx.Amount < 0:
                from_tx, to_tx = tx, other_pending.transaction
            else:
                from_tx, to_tx = other_pending.transaction, tx

            exchange_events.append(
                RevolutExchangeEvent(
                    event_date=from_tx.StartedDate,
                    from_currency=from_tx.currency,
                    to_currency=to_tx.currency,
                    from_amount=abs(from_tx.Amount),
                    to_amount=abs(to_tx.Amount),
                )
            )
        else:
            exchange_pending[pair_key] = RevolutExchangePending(
                raw_id=raw_row.raw_id,
                transaction=tx,
            )

    for unmatched_pending in exchange_pending.values():
        exchange_row_ids.add(unmatched_pending.raw_id)

    # Step 2: build a simple timeline of inferred CHF conversion rates based on
    # the observed exchange events, starting from CHF = 1.0.
    exchange_events.sort(key=lambda event: event.event_date)
    running_rates: dict[str, float] = {"CHF": 1.0}
    rate_timeline: dict[str, list[RevolutRatePoint]] = defaultdict(list)

    for event in exchange_events:
        chf_cost = event.from_amount * running_rates.get(event.from_currency, 1.0)
        if event.to_amount > 0:
            new_rate = chf_cost / event.to_amount
            running_rates[event.to_currency] = new_rate
            rate_timeline[event.to_currency].append(
                RevolutRatePoint(
                    event_date=event.event_date,
                    rate_to_chf=new_rate,
                )
            )

    def _rate_at(currency: str, date: datetime) -> float:
        if currency == "CHF":
            return 1.0

        entries = rate_timeline.get(currency, [])
        rate = 1.0
        for entry in entries:
            if entry.event_date <= date:
                rate = entry.rate_to_chf
            else:
                break
        return rate

    # Step 3: apply the most recent known rate to each non-exchange row and
    # return its signed CHF amount. Exchange legs are returned as None so they
    # can be skipped by the refined stage.
    result: dict[int, float | None] = {}
    for raw_row, tx in rows:
        if raw_row.raw_id in exchange_row_ids:
            result[raw_row.raw_id] = None
            continue

        tx_date = tx.CompletedDate or tx.StartedDate
        result[raw_row.raw_id] = tx.Amount * _rate_at(tx.currency, tx_date)

    return result


def _is_duplicate(
    db: sqlite3.Connection,
    reference: str,
    date_iso: str | None,
    amount: float,
) -> bool:
    """Find duplicates in transactions_rfn based on reference, date, and amount."""
    params: tuple[str, float] | tuple[str, str, float]
    if date_iso is None:
        query = """
			SELECT 1
			FROM transactions_rfn
			WHERE reference = ?
			  AND date IS NULL
			  AND amount = ?
			LIMIT 1
		"""
        params = (reference.strip(), amount)
    else:
        query = """
			SELECT 1
			FROM transactions_rfn
			WHERE reference = ?
			  AND date = ?
			  AND amount = ?
			LIMIT 1
		"""
        params = (reference.strip(), date_iso, amount)

    row: tuple[object, ...] | None = db.execute(query, params).fetchone()

    return row is not None


def _get_revolut_chf_map(
    rows: list[RawRow], source_type: SourceType = SourceType.REVOLUT
) -> dict[int, float | None]:
    revolut_chf_map: dict[int, float | None] = {}
    model_class = SOURCE_MODEL_MAP[source_type]
    pre_parsed: list[tuple[RawRow, RevolutTransaction]] = cast(
        "list[tuple[RawRow, RevolutTransaction]]",
        [(row, model_class.model_validate(json.loads(row.raw_json))) for row in rows],
    )
    revolut_chf_map = _resolve_revolut_chf_amounts(pre_parsed)

    return revolut_chf_map


def _clean_debit_ebanking(
    zkb_model: ZKBTransaction,
    context: DebitEBankingContext,
) -> ZKBTransaction | None:
    """Normalize ZKB debit eBanking parent/detail rows.

    Returns the cleaned transaction row. Parent helper rows return `None`
    because they only provide context for following detail rows and should not
    be inserted into `transactions_rfn`.
    """
    booking_text = zkb_model.BookingText.strip()

    if re.fullmatch(r"Debit eBanking(?: Mobile)? \(\d+\)", booking_text):
        context.parent_date = zkb_model.Date or zkb_model.ValueDate
        context.parent_reference = (
            zkb_model.ZKBReference or zkb_model.ReferenceNumber or None
        )
        context.detail_counter = 0
        return None

    is_ebanking_detail = (
        zkb_model.Date is None
        and zkb_model.AmountDebit is None
        and zkb_model.AmountCredit is None
        and zkb_model.AmountDetails is not None
        and zkb_model.Curr is not None
    )
    if is_ebanking_detail and context.parent_date is not None:
        zkb_model.Date = context.parent_date
        if context.parent_reference is not None:
            context.detail_counter += 1
            zkb_model.ZKBReference = (
                f"{context.parent_reference}-{context.detail_counter}"
            )
        return zkb_model

    if zkb_model.Date is not None:
        context.parent_date = None
        context.parent_reference = None
        context.detail_counter = 0

    return zkb_model


def process_refined_source(source_type: SourceType) -> dict[str, int]:
    """Transform unprocessed raw rows of a source into refined transactions.

    High-level flow:
    1. Ensure schema exists and load all unprocessed raw rows for the source.
    2. Precompute source-specific context (e.g. Revolut CHF conversion map).
    3. Parse each raw row into its source model and apply source-specific rules.
    4. Skip rows that should not become refined records (duplicates, exchange
       legs, topups, helper parent rows) while still marking them processed.
    5. Insert normalized rows into `transactions_rfn` and mark raw rows as
       processed.
    6. Promote ingested file status to `refined` when all file rows are fully
       processed.

    Returns counters for visibility into refinement throughput.
    """
    create_all_tables()

    # Step 1: load candidate raw rows and source adapters.
    rows = _load_unprocessed_raw_rows(source_type)
    source_adapter_map = get_source_adapter_map()

    # Step 2: Get revolut CHF conversion map if processing Revolut rows
    revolut_chf_map: dict[int, float | None] = {}
    if source_type == SourceType.REVOLUT:
        revolut_chf_map = _get_revolut_chf_map(rows)

    rows_processed = 0
    records_inserted = 0
    duplicates_found = 0
    processed_file_ids: set[int] = set()
    zkb_ebanking_context = DebitEBankingContext()

    with get_connection() as db:

        def mark_processed(raw_id: int, file_id: int) -> None:
            """Mark a raw row as processed and update local bookkeeping counters."""
            nonlocal rows_processed
            db.execute(
                "UPDATE transactions_raw SET processed = 1 WHERE id = ?",
                (raw_id,),
            )
            processed_file_ids.add(file_id)
            rows_processed += 1

        # Step 3: row-by-row parse, source-rule handling, dedup, and insertion.
        for row in rows:
            current_source = SourceType(row.source_type)
            model_class = SOURCE_MODEL_MAP[current_source]
            adapter = source_adapter_map[current_source]

            payload = json.loads(row.raw_json)
            source_model = model_class.model_validate(payload)

            # Step 3a: ZKB debit eBanking
            if source_type == SourceType.ZKB_DEBIT:
                zkb_model = cast("ZKBTransaction", source_model)
                cleaned_zkb_model = _clean_debit_ebanking(
                    zkb_model,
                    zkb_ebanking_context,
                )
                if cleaned_zkb_model is None:
                    mark_processed(row.raw_id, row.file_id)
                    continue
                source_model = cleaned_zkb_model

            # Step 3b: Revolut processing
            revolut_chf_signed: float | None = None
            if source_type == SourceType.REVOLUT:
                # Exchange legs are pre-marked as `None` in the CHF map and
                # should be skipped from refined insertion.
                chf_candidate = revolut_chf_map.get(row.raw_id)
                if chf_candidate is None:
                    mark_processed(row.raw_id, row.file_id)
                    continue

                revolut_source_model = cast("RevolutTransaction", source_model)
                # Topups are balance movements, not spending/income records for
                # the refined transaction table.
                if revolut_source_model.Type.lower() == "topup":
                    mark_processed(row.raw_id, row.file_id)
                    continue

                # Keep the signed CHF value so we can derive both absolute
                # amount and transaction direction after unification.
                revolut_chf_signed = chf_candidate

            # Step 3c: convert the source-specific model into the common
            # unified transaction shape used by downstream refinement logic.
            unified = adapter.to_unified(source_model, source_file=row.source_file)

            if source_type == SourceType.REVOLUT and revolut_chf_signed is not None:
                # Revolut rows are stored in CHF using the precomputed signed
                # amount. The sign determines expense vs. income.
                amount = abs(revolut_chf_signed)
                transaction_type = (
                    TransactionType.EXPENSE
                    if revolut_chf_signed < 0
                    else TransactionType.INCOME
                ).value
                currency = Currency.CHF.value
            else:
                # Non-Revolut sources already provide the final amount,
                # transaction type, and currency through the unified model.
                amount = unified.amount
                transaction_type = unified.transaction_type.value
                currency = unified.currency.value

            # Step 3d: normalize final values before deduplication and insert.
            amount = round(amount, 2)
            date_iso = _as_iso_datetime(unified.date)
            reference = unified.zkb_reference
            is_duplicate = _is_duplicate(
                db,
                reference,
                date_iso,
                amount,
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

            db.execute(
                """
				INSERT INTO transactions_rfn (
					raw_id,
					source_type,
					date,
					amount,
					transaction_type,
					booking_text,
					merchant_normalized,
					is_person,
					currency,
					reference,
					enrichment_status,
					created_at
				)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				""",
                (
                    row.raw_id,
                    SourceType(unified.source).value,
                    date_iso,
                    amount,
                    transaction_type,
                    unified.booking_text,
                    merchant_normalized,
                    int(is_person),
                    currency,
                    reference,
                    enrichment_status,
                    datetime.now().isoformat(),
                ),
            )

            mark_processed(row.raw_id, row.file_id)
            records_inserted += 1

        # Step 4: close ingested files whose raw rows are now fully processed.
        for file_id in processed_file_ids:
            unprocessed_count = db.execute(
                """
				SELECT COUNT(*)
				FROM transactions_raw tr
				JOIN transactions_lnd tl ON tl.id = tr.landing_id
				WHERE tl.file_id = ?
				  AND tr.processed = 0
				""",
                (file_id,),
            ).fetchone()
            if unprocessed_count is None or int(unprocessed_count[0]) > 0:
                continue

            db.execute(
                "UPDATE ingested_files SET status = 'refined' WHERE id = ?",
                (file_id,),
            )

    return {
        "rows_found": len(rows),
        "rows_processed": rows_processed,
        "records_inserted": records_inserted,
        "duplicates_found": duplicates_found,
    }


def run_refined() -> dict[str, dict[str, int]]:
    results: dict[str, dict[str, int]] = {}

    for source_type in SOURCE_MODEL_MAP:
        result = process_refined_source(source_type)
        results[source_type.value] = result

    return results
