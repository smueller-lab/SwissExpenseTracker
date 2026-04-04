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
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection


def _normalize_merchant(merchant: str) -> str:
    normalized = merchant.lower()
    normalized = normalized.replace("ü", "u").replace("ä", "a").replace("ö", "o")

    for pattern, canonical in MERCHANT_COMPOUND_BRANDS:
        if pattern in normalized:
            return canonical

    for brand in MERCHANT_BRANDS:
        if brand in normalized:
            return brand

    name = re.sub(r"\d+", "", normalized)
    name = re.sub(r"[^a-z\s]", " ", name)
    name = re.sub(r"\b(ag|sa|gmbh|ltd)\b", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name if name else merchant


def _booking_text_split(text: str) -> str:
    if "TWINT" in text:
        return text.split(":", 1)[-1]
    if "ZKB Visa Debit" in text:
        return text.split(",", 1)[-1]
    if "Debit eBanking" in text:
        return text.split(":", 1)[-1]
    if "ZKB Mastro card" in text:
        return text.split(",", 1)[-1]
    return text.split(":", 1)[-1]


def _extract_merchant_normalized(booking_text: str | None) -> str | None:
    if not booking_text:
        return None

    split_text = _booking_text_split(booking_text).strip()
    if not split_text:
        return None

    return _normalize_merchant(split_text)


def _as_iso_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _load_unprocessed_raw_rows(source_type: SourceType) -> list[RawRow]:
    query = """
		SELECT tr.id, tr.landing_id, tl.file_id, tr.source_type, tr.raw_json, tr.source_file
		FROM transactions_raw tr
		JOIN transactions_landing tl ON tl.id = tr.landing_id
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
    exchange_pending: dict[tuple[str, str], tuple[int, RevolutTransaction]] = {}
    exchange_row_ids: set[int] = set()
    exchange_events: list[tuple[datetime, str, str, float, float]] = []

    for raw_row, tx in rows:
        if tx.Type.lower() != "exchange":
            continue

        pair_key = (tx.Description, tx.StartedDate.isoformat())
        if pair_key in exchange_pending:
            other_raw_id, other_tx = exchange_pending.pop(pair_key)
            exchange_row_ids.add(raw_row.raw_id)
            exchange_row_ids.add(other_raw_id)

            if tx.Amount < 0:
                from_tx, to_tx = tx, other_tx
            else:
                from_tx, to_tx = other_tx, tx

            exchange_events.append(
                (
                    from_tx.StartedDate,
                    from_tx.currency,
                    to_tx.currency,
                    abs(from_tx.Amount),
                    abs(to_tx.Amount),
                )
            )
        else:
            exchange_pending[pair_key] = (raw_row.raw_id, tx)

    for unmatched_raw_id, _ in exchange_pending.values():
        exchange_row_ids.add(unmatched_raw_id)

    exchange_events.sort(key=lambda event: event[0])
    running_rates: dict[str, float] = {"CHF": 1.0}
    rate_timeline: dict[str, list[tuple[datetime, float]]] = defaultdict(list)

    for event_date, from_curr, to_curr, from_amt, to_amt in exchange_events:
        chf_cost = from_amt * running_rates.get(from_curr, 1.0)
        if to_amt > 0:
            new_rate = chf_cost / to_amt
            running_rates[to_curr] = new_rate
            rate_timeline[to_curr].append((event_date, new_rate))

    def _rate_at(currency: str, date: datetime) -> float:
        if currency == "CHF":
            return 1.0

        entries = rate_timeline.get(currency, [])
        rate = 1.0
        for entry_date, entry_rate in entries:
            if entry_date <= date:
                rate = entry_rate
            else:
                break
        return rate

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
    params: tuple[str, float] | tuple[str, str, float]
    if date_iso is None:
        query = """
			SELECT 1
			FROM transactions_refined
			WHERE reference = ?
			  AND date IS NULL
			  AND amount = ?
			LIMIT 1
		"""
        params = (reference.strip(), amount)
    else:
        query = """
			SELECT 1
			FROM transactions_refined
			WHERE reference = ?
			  AND date = ?
			  AND amount = ?
			LIMIT 1
		"""
        params = (reference.strip(), date_iso, amount)

    row: tuple[object, ...] | None = db.execute(query, params).fetchone()

    return row is not None


def process_refined_source(source_type: SourceType) -> dict[str, int]:
    create_all_tables()
    rows = _load_unprocessed_raw_rows(source_type)
    source_adapter_map = get_source_adapter_map()

    revolut_chf_map: dict[int, float | None] = {}
    if source_type == SourceType.REVOLUT:
        model_class = SOURCE_MODEL_MAP[source_type]
        pre_parsed: list[tuple[RawRow, RevolutTransaction]] = cast(
            "list[tuple[RawRow, RevolutTransaction]]",
            [
                (row, model_class.model_validate(json.loads(row.raw_json)))
                for row in rows
            ],
        )
        revolut_chf_map = _resolve_revolut_chf_amounts(pre_parsed)

    rows_processed = 0
    records_inserted = 0
    duplicates_found = 0
    processed_file_ids: set[int] = set()

    with get_connection() as db:
        for row in rows:
            current_source = SourceType(row.source_type)
            model_class = SOURCE_MODEL_MAP[current_source]
            adapter = source_adapter_map[current_source]

            payload = json.loads(row.raw_json)
            source_model = model_class.model_validate(payload)

            revolut_chf_signed: float | None = None
            if source_type == SourceType.REVOLUT:
                chf_candidate = revolut_chf_map.get(row.raw_id)
                if chf_candidate is None:
                    db.execute(
                        "UPDATE transactions_raw SET processed = 1 WHERE id = ?",
                        (row.raw_id,),
                    )
                    processed_file_ids.add(row.file_id)
                    rows_processed += 1
                    continue

                revolut_source_model = cast("RevolutTransaction", source_model)
                if revolut_source_model.Type.lower() == "topup":
                    db.execute(
                        "UPDATE transactions_raw SET processed = 1 WHERE id = ?",
                        (row.raw_id,),
                    )
                    processed_file_ids.add(row.file_id)
                    rows_processed += 1
                    continue

                revolut_chf_signed = chf_candidate

            unified = adapter.to_unified(source_model, source_file=row.source_file)

            if source_type == SourceType.REVOLUT and revolut_chf_signed is not None:
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
                duplicates_found += 1
                db.execute(
                    "UPDATE transactions_raw SET processed = 1 WHERE id = ?",
                    (row.raw_id,),
                )
                processed_file_ids.add(row.file_id)
                rows_processed += 1
                continue

            merchant_normalized = _extract_merchant_normalized(unified.booking_text)

            enrichment_status = EnrichmentStatus.PENDING.value

            db.execute(
                """
				INSERT INTO transactions_refined (
					raw_id,
					source_type,
					date,
					amount,
					transaction_type,
					booking_text,
					merchant_normalized,
					currency,
					reference,
					enrichment_status,
					created_at
				)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				""",
                (
                    row.raw_id,
                    SourceType(unified.source).value,
                    date_iso,
                    amount,
                    transaction_type,
                    unified.booking_text,
                    merchant_normalized,
                    currency,
                    reference,
                    enrichment_status,
                    datetime.now().isoformat(),
                ),
            )

            db.execute(
                "UPDATE transactions_raw SET processed = 1 WHERE id = ?",
                (row.raw_id,),
            )

            processed_file_ids.add(row.file_id)
            rows_processed += 1
            records_inserted += 1

        for file_id in processed_file_ids:
            unprocessed_count = db.execute(
                """
				SELECT COUNT(*)
				FROM transactions_raw tr
				JOIN transactions_landing tl ON tl.id = tr.landing_id
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
