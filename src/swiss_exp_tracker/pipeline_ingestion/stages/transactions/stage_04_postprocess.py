from __future__ import annotations

import sqlite3

from pathlib import Path

import yaml

from swiss_exp_tracker.config.user_config_schema import CreditCardPaymentsDetected
from swiss_exp_tracker.config.user_config_schema import DetectedRules
from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType
from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db import get_connection
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.credit_card_matcher import (
    CreditCardPaymentMatch,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.credit_card_matcher import (
    find_credit_card_payment_pairs,
)

_DETECTED_RULES_PATH: Path = (
    Path(__file__).parent.parent.parent.parent
    / "pipeline_agentic"
    / "config"
    / "detected_rules.yaml"
)


def _record_credit_card_payments(
    matches: list[CreditCardPaymentMatch],
    output_path: Path = _DETECTED_RULES_PATH,
) -> None:
    """Persist the matched legs' distinct booking texts into detected_rules.yaml; keeps salary/housing."""
    if not matches:
        return

    existing = DetectedRules()
    if output_path.exists():
        with output_path.open(encoding="utf-8") as f:
            raw: object = yaml.safe_load(f) or {}
        existing = DetectedRules.model_validate(raw)

    texts: set[str] = set()
    for m in matches:
        for text in (m.debit_text, m.credit_text):
            if text and text.strip():
                texts.add(text.strip())

    updated = DetectedRules(
        salary=existing.salary,
        housing=existing.housing,
        credit_card_payments=CreditCardPaymentsDetected(booking_texts=sorted(texts)),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(updated.model_dump(), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _clean_credit_card_payments(db: sqlite3.Connection) -> int:
    """Remove matched credit-card payment pairs from transactions_rfn; returns pair count removed."""
    matches = find_credit_card_payment_pairs(db)

    if not matches:
        return 0

    _record_credit_card_payments(matches)

    ids_to_delete: list[int] = []
    for match in matches:
        ids_to_delete.append(match.debit_id)
        ids_to_delete.append(match.credit_id)

    # dynamic IN clause: cannot be expressed as static aiosql SQL
    placeholders = ",".join("?" for _ in ids_to_delete)
    db.execute(
        f"DELETE FROM transactions_rfn WHERE id IN ({placeholders})",
        tuple(ids_to_delete),
    )
    return len(matches)


def _fill_viseca_credit_card_fee_text(db: sqlite3.Connection) -> int:
    """Backfill booking_text and merchant for Viseca fee rows; returns rows updated."""
    rowcount: int = transactions.fill_viseca_fee_text(
        db,
        booking_text="Credit card fee",
        merchant_normalized="credit card fee",
        source_type=SourceType.VISECA.value,
        transaction_type=TransactionType.EXPENSE.value,
        amount=2.0,
    )
    return max(rowcount, 0)


def run_postprocess() -> dict[str, int]:
    """Run postprocessing: remove credit card payment pairs and fill Viseca fee text."""
    create_all_tables()

    with get_connection() as db:
        credit_card_pairs_removed = _clean_credit_card_payments(db)
        viseca_fee_rows_updated = _fill_viseca_credit_card_fee_text(db)

    return {
        "credit_card_pairs_removed": credit_card_pairs_removed,
        "viseca_fee_rows_updated": viseca_fee_rows_updated,
    }
