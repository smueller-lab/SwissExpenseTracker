from __future__ import annotations

import sqlite3

from collections import defaultdict
from datetime import date as dt_date
from datetime import datetime

from pydantic import BaseModel
from pydantic import field_validator

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import (
    get_credit_card_source_pairs,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType

# Maximum allowed gap in days between a debit leg and a credit leg to form a matched pair.
_MAX_PAIR_DAYS = 7


class CreditCardPaymentMatch(BaseModel):
    """A matched pair of debit and credit legs representing a credit card settlement."""

    debit_id: int
    credit_id: int
    debit_source: str
    credit_source: str
    amount: float
    date: str
    is_lsv: bool = False
    debit_text: str | None = None
    credit_text: str | None = None

    @field_validator("date", mode="before")
    @classmethod
    def _validate_date(cls, value: object) -> object:
        """Validate that a non-empty date value is a parseable ISO 'YYYY-MM-DD' string."""
        if isinstance(value, str) and value:
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError as exc:
                msg = f"Cannot parse date: {value!r}"
                raise ValueError(msg) from exc
        return value


def _parse_date_safe(date_str: str | None) -> dt_date | None:
    """Parse an ISO date or datetime string to a date; returns None if absent or unparseable.
    Accepts both 'YYYY-MM-DD' and 'YYYY-MM-DDThh:mm:ss' (the rfn table stores the latter).
    """
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str).date()
    except ValueError:
        return None


def _iso_day(date_str: str | None) -> str:
    """Normalise a raw rfn date/datetime string to a 'YYYY-MM-DD' string; '' if unparseable."""
    parsed = _parse_date_safe(date_str)
    return parsed.isoformat() if parsed else ""


def _row_date(date_str: str | None) -> str:
    """Return a sort key for a nullable date string; absent dates sort last."""
    return date_str if date_str else "9999-99-99"


def find_credit_card_payment_pairs(
    db: sqlite3.Connection,
) -> list[CreditCardPaymentMatch]:
    """Return matched debit/credit leg pairs for all credit-card source pairs in the DB.

    Pairs a debit-source EXPENSE with a credit-source INCOME of equal amount (rounded to
    2 dp) within _MAX_PAIR_DAYS, regardless of booking text; each rfn id matches at most once.
    """
    used_ids: set[int] = set()
    matches: list[CreditCardPaymentMatch] = []

    for debit_source, credit_source in get_credit_card_source_pairs():
        debit_rows = transactions.get_rfn_rows_by_source_and_type(
            db,
            source_type=debit_source.value,
            transaction_type=TransactionType.EXPENSE.value,
        )
        credit_rows = transactions.get_rfn_rows_by_source_and_type(
            db,
            source_type=credit_source.value,
            transaction_type=TransactionType.INCOME.value,
        )

        # Group by rounded amount; exclude IDs already consumed by a prior source pair.
        # Each leg is (id, date, booking_text).
        debit_by_amount: dict[float, list[tuple[int, str | None, str | None]]] = (
            defaultdict(list)
        )
        for row in debit_rows:
            rid = int(row[0])
            if rid not in used_ids:
                debit_by_amount[round(float(row[1]), 2)].append(
                    (rid, row[2] if row[2] else None, row[3] if row[3] else None)
                )

        credit_by_amount: dict[float, list[tuple[int, str | None, str | None]]] = (
            defaultdict(list)
        )
        for row in credit_rows:
            rid = int(row[0])
            if rid not in used_ids:
                credit_by_amount[round(float(row[1]), 2)].append(
                    (rid, row[2] if row[2] else None, row[3] if row[3] else None)
                )

        for amt_key, debits in debit_by_amount.items():
            credits = credit_by_amount.get(amt_key)
            if not credits:
                continue

            # Sort both lists by (date, id) for deterministic, order-stable output.
            debits_sorted = sorted(debits, key=lambda r: (_row_date(r[1]), r[0]))
            credits_sorted = sorted(credits, key=lambda r: (_row_date(r[1]), r[0]))

            # Track which credit legs remain available within this amount bucket.
            available: dict[int, str | None] = {
                cid: text for cid, _, text in credits_sorted
            }

            for debit_id, debit_date_str, debit_text in debits_sorted:
                debit_date = _parse_date_safe(debit_date_str)

                best_credit_id: int | None = None
                best_credit_text: str | None = None
                best_diff: int | None = None

                for credit_id, credit_date_str, credit_text in credits_sorted:
                    if credit_id not in available:
                        continue
                    credit_date = _parse_date_safe(credit_date_str)
                    # A leg with no parseable date cannot satisfy the 7-day window.
                    if debit_date is None or credit_date is None:
                        continue
                    diff = abs((debit_date - credit_date).days)
                    if diff <= _MAX_PAIR_DAYS and (
                        best_diff is None or diff < best_diff
                    ):
                        best_diff = diff
                        best_credit_id = credit_id
                        best_credit_text = credit_text

                if best_credit_id is not None:
                    used_ids.add(debit_id)
                    used_ids.add(best_credit_id)
                    del available[best_credit_id]
                    combined_text = f"{debit_text or ''} {best_credit_text or ''}"
                    matches.append(
                        CreditCardPaymentMatch(
                            debit_id=debit_id,
                            credit_id=best_credit_id,
                            debit_source=debit_source.value,
                            credit_source=credit_source.value,
                            amount=amt_key,
                            date=_iso_day(debit_date_str),
                            is_lsv="lsv" in combined_text.lower(),
                            debit_text=debit_text,
                            credit_text=best_credit_text,
                        )
                    )

    return matches
