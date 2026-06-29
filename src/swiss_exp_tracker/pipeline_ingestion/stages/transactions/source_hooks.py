from __future__ import annotations

import json
import re

from collections import defaultdict
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from swiss_exp_tracker.pipeline_ingestion.adapters.coercion import parse_swiss_float
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import (
    SourceProfile,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.tables import RawRow

_ZKB_EBANKING_PATTERN = re.compile(
    r"Debit (?:eBanking(?: Mobile)?|Mobile Banking|Standing order)(?: \(\d+\))?"
)


class DebitEBankingContext(BaseModel):
    """Track ZKB eBanking parent/detail parsing state across sequential rows."""

    parent_date_str: str | None = None
    parent_reference: str | None = None
    detail_counter: int = 0


class _RevolutPending(BaseModel):
    """Hold an unmatched Revolut exchange row while waiting for its pair."""

    raw_id: int
    row_dict: dict[str, Any]


class _ExchangeEvent(BaseModel):
    """Record one paired Revolut currency exchange for rate-timeline reconstruction."""

    event_date: datetime
    from_currency: str
    to_currency: str
    from_amount: float
    to_amount: float


class _RatePoint(BaseModel):
    """One entry in the per-currency CHF rate timeline."""

    event_date: datetime
    rate_to_chf: float


def clean_zkb_ebanking(
    row_dict: dict[str, Any],
    context: DebitEBankingContext,
    profile: SourceProfile,
) -> dict[str, Any] | None:
    """Process one ZKB row through the eBanking parent/detail state machine.

    Returns None for parent rows (caller skips); returns the row dict (possibly
    mutated with inherited date and composite reference) for detail and normal rows.
    """
    primary_date_col = profile.date_columns[0] if profile.date_columns else "Date"
    debit_col = profile.amount.debit_column
    credit_col = profile.amount.credit_column
    fallback_col = profile.amount.fallback_amount_column
    currency_col = profile.currency.column
    ref_col = profile.reference_columns[0] if profile.reference_columns else None

    booking_text = str(row_dict.get("Booking text", "") or "").strip()

    if _ZKB_EBANKING_PATTERN.fullmatch(booking_text):
        # Parent row: capture date and reference for following detail rows.
        parent_date_str: str | None = None
        for col in profile.date_columns:
            val = row_dict.get(col)
            if val not in (None, ""):
                parent_date_str = str(val)
                break
        context.parent_date_str = parent_date_str

        ref_str: str | None = None
        for col in profile.reference_columns:
            val = row_dict.get(col)
            if val not in (None, ""):
                ref_str = str(val)
                break
        context.parent_reference = ref_str
        context.detail_counter = 0
        return None

    # Detail row: primary date empty, debit/credit empty, fallback amount and currency present.
    is_detail = (
        row_dict.get(primary_date_col) in (None, "")
        and (debit_col is None or row_dict.get(debit_col) in (None, ""))
        and (credit_col is None or row_dict.get(credit_col) in (None, ""))
        and fallback_col is not None
        and row_dict.get(fallback_col) not in (None, "")
        and currency_col is not None
        and row_dict.get(currency_col) not in (None, "")
    )
    if is_detail and context.parent_date_str is not None:
        row_dict[primary_date_col] = context.parent_date_str
        if context.parent_reference is not None and ref_col is not None:
            context.detail_counter += 1
            row_dict[ref_col] = f"{context.parent_reference}-{context.detail_counter}"
        return row_dict

    # Normal row: reset context so detail detection does not leak past this group.
    if row_dict.get(primary_date_col) not in (None, ""):
        context.parent_date_str = None
        context.parent_reference = None
        context.detail_counter = 0

    return row_dict


def _parse_iso_dt(value: Any) -> datetime | None:
    """Parse an ISO datetime string to datetime; return None for empty or None values."""
    if value in (None, ""):
        return None
    return datetime.fromisoformat(str(value))


def resolve_revolut_chf_amounts(
    rows: list[tuple[RawRow, dict[str, Any]]],
    profile: SourceProfile,
) -> dict[int, float | None]:
    """Return raw_id → signed CHF amount for all Revolut rows.

    Exchange pair rows are mapped to None (caller skips them). All other rows get a
    signed CHF amount derived from the most recent known exchange rate at their date.
    """
    currency_col = profile.currency.column or "Currency"
    amount_col = profile.amount.amount_column or "Amount"

    exchange_pending: dict[tuple[str, str], _RevolutPending] = {}
    exchange_row_ids: set[int] = set()
    exchange_events: list[_ExchangeEvent] = []

    for raw_row, row_dict in rows:
        if str(row_dict.get("Type", "")).lower() != "exchange":
            continue

        desc = str(row_dict.get("Description", ""))
        started_str = str(row_dict.get("Started Date", ""))
        pair_key = (desc, started_str)
        amount = float(parse_swiss_float(row_dict.get(amount_col)) or 0.0)
        currency = str(row_dict.get(currency_col, ""))

        if pair_key in exchange_pending:
            other = exchange_pending.pop(pair_key)
            exchange_row_ids.add(raw_row.raw_id)
            exchange_row_ids.add(other.raw_id)

            other_amount = float(
                parse_swiss_float(other.row_dict.get(amount_col)) or 0.0
            )
            other_currency = str(other.row_dict.get(currency_col, ""))

            if amount < 0:
                from_amount, from_currency = abs(amount), currency
                to_amount, to_currency = abs(other_amount), other_currency
            else:
                from_amount, from_currency = abs(other_amount), other_currency
                to_amount, to_currency = abs(amount), currency

            started_dt = _parse_iso_dt(started_str)
            if started_dt is not None:
                exchange_events.append(
                    _ExchangeEvent(
                        event_date=started_dt,
                        from_currency=from_currency,
                        to_currency=to_currency,
                        from_amount=from_amount,
                        to_amount=to_amount,
                    )
                )
        else:
            exchange_pending[pair_key] = _RevolutPending(
                raw_id=raw_row.raw_id,
                row_dict=row_dict,
            )

    for unmatched in exchange_pending.values():
        exchange_row_ids.add(unmatched.raw_id)

    exchange_events.sort(key=lambda e: e.event_date)
    running_rates: dict[str, float] = {"CHF": 1.0}
    rate_timeline: dict[str, list[_RatePoint]] = defaultdict(list)

    for event in exchange_events:
        chf_cost = event.from_amount * running_rates.get(event.from_currency, 1.0)
        if event.to_amount > 0:
            new_rate = chf_cost / event.to_amount
            running_rates[event.to_currency] = new_rate
            rate_timeline[event.to_currency].append(
                _RatePoint(event_date=event.event_date, rate_to_chf=new_rate)
            )

    def _rate_at(currency: str, date: datetime) -> float:
        """Return the CHF rate for currency at date using the timeline, defaulting to 1.0."""
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

    result: dict[int, float | None] = {}
    for raw_row, row_dict in rows:
        if raw_row.raw_id in exchange_row_ids:
            result[raw_row.raw_id] = None
            continue

        amount = float(parse_swiss_float(row_dict.get(amount_col)) or 0.0)
        currency = str(row_dict.get(currency_col, ""))
        completed_dt = _parse_iso_dt(row_dict.get("Completed Date"))
        started_dt = _parse_iso_dt(row_dict.get("Started Date"))
        tx_date = completed_dt or started_dt or datetime.min
        result[raw_row.raw_id] = amount * _rate_at(currency, tx_date)

    return result


def get_revolut_chf_map(
    rows: list[RawRow],
    profile: SourceProfile,
) -> dict[int, float | None]:
    """Build raw_id → signed CHF amount map for all Revolut rows."""
    pre_parsed: list[tuple[RawRow, dict[str, Any]]] = [
        (row, json.loads(row.raw_json)) for row in rows
    ]
    return resolve_revolut_chf_amounts(pre_parsed, profile)
