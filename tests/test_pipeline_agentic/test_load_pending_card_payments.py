"""Tests for card-payment filtering in load_pending_transactions (pipeline.py)."""

from __future__ import annotations

import sqlite3

from datetime import datetime
from pathlib import Path

import pytest

from swiss_exp_tracker.pipeline_agentic.pipeline import load_pending_transactions

# ---------------------------------------------------------------------------
# Factory and DB helpers
# ---------------------------------------------------------------------------


def _make_rfn_row(**overrides: object) -> dict[str, object]:
    """Return a valid rfn base dict; each test only states what it varies."""
    base: dict[str, object] = {
        "source_type": "ZKB_DEBIT",
        "date": "2025-06-01",
        "amount": 50.0,
        "transaction_type": "EXPENSE",
        "booking_text": "COOP-2440 ZUERICH",
        "merchant_normalized": "coop",
        "is_person": 0,
        "currency": "CHF",
        "reference": "REF-001",
        "enrichment_status": "pending",
    }
    base.update(overrides)
    return base


def _insert_rfn(db_path: Path, row: dict[str, object]) -> int:
    """Insert the full FK chain + one rfn row into db_path; return the rfn id."""
    source_type = str(row["source_type"])
    filename = f"test_{source_type}.csv"
    file_hash = f"hash_{source_type}"

    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT OR IGNORE INTO ingested_files "
            "(filename, file_hash, source_type, ingested_at, record_count, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                filename,
                file_hash,
                source_type,
                datetime.now().isoformat(),
                1,
                "refined",
            ),
        )
        file_id = db.execute(
            "SELECT id FROM ingested_files WHERE filename=? AND source_type=? "
            "ORDER BY id DESC LIMIT 1",
            (filename, source_type),
        ).fetchone()[0]

        db.execute(
            "INSERT INTO transactions_lnd "
            "(file_id, source_type, raw_json, created_at, processed) "
            "VALUES (?, ?, ?, ?, 1)",
            (file_id, source_type, "{}", datetime.now().isoformat()),
        )
        lnd_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

        db.execute(
            "INSERT INTO transactions_raw "
            "(landing_id, source_type, raw_json, source_file, created_at, processed) "
            "VALUES (?, ?, ?, ?, ?, 1)",
            (lnd_id, source_type, "{}", filename, datetime.now().isoformat()),
        )
        raw_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

        db.execute(
            "INSERT INTO transactions_rfn "
            "(raw_id, source_type, date, amount, transaction_type, "
            " booking_text, merchant_normalized, is_person, currency, "
            " reference, enrichment_status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                raw_id,
                source_type,
                row.get("date"),
                row["amount"],
                row["transaction_type"],
                row.get("booking_text"),
                row.get("merchant_normalized"),
                row.get("is_person", 0),
                row.get("currency"),
                row.get("reference"),
                row.get("enrichment_status", "pending"),
                datetime.now().isoformat(),
            ),
        )
        rfn_id = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
        db.commit()
    return rfn_id


def _get_enrichment_status(db_path: Path, rfn_id: int) -> str:
    """Read enrichment_status for the given rfn row directly from the DB."""
    with sqlite3.connect(db_path) as db:
        row = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE id = ?",
            (rfn_id,),
        ).fetchone()
    assert row is not None, f"rfn row {rfn_id} not found"
    return str(row[0])


# ---------------------------------------------------------------------------
# Card-payment exclusion
# ---------------------------------------------------------------------------


def test_card_payment_row_excluded_and_status_stays_pending(
    tmp_db: Path, mocker: pytest.MockerFixture
) -> None:
    """Pending row matching a detected card-payment text is excluded from result but stays pending."""
    mocker.patch(
        "swiss_exp_tracker.pipeline_agentic.pipeline.load_credit_card_payment_texts",
        return_value=["Ihre Zahlung - Danke"],
    )
    rfn_id = _insert_rfn(tmp_db, _make_rfn_row(booking_text="Ihre Zahlung - Danke"))

    result = load_pending_transactions()

    refined_ids = [tx.refined_id for tx in result]
    assert rfn_id not in refined_ids
    assert _get_enrichment_status(tmp_db, rfn_id) == "pending"


def test_normal_merchant_not_excluded(
    tmp_db: Path, mocker: pytest.MockerFixture
) -> None:
    """Pending row with a non-card-payment booking_text is returned as a Transaction."""
    mocker.patch(
        "swiss_exp_tracker.pipeline_agentic.pipeline.load_credit_card_payment_texts",
        return_value=["Ihre Zahlung - Danke"],
    )
    rfn_id = _insert_rfn(tmp_db, _make_rfn_row(booking_text="COOP-2440 ZUERICH"))

    result = load_pending_transactions()

    refined_ids = [tx.refined_id for tx in result]
    assert rfn_id in refined_ids


def test_card_payment_exclusion_is_case_and_whitespace_insensitive(
    tmp_db: Path, mocker: pytest.MockerFixture
) -> None:
    """Exclusion is case- and whitespace-insensitive: padded/mixed-case text is excluded."""
    mocker.patch(
        "swiss_exp_tracker.pipeline_agentic.pipeline.load_credit_card_payment_texts",
        return_value=["Ihre Zahlung - Danke"],
    )
    rfn_id = _insert_rfn(
        tmp_db, _make_rfn_row(booking_text="  ihre   zahlung - danke ")
    )

    result = load_pending_transactions()

    refined_ids = [tx.refined_id for tx in result]
    assert rfn_id not in refined_ids
    assert _get_enrichment_status(tmp_db, rfn_id) == "pending"


def test_empty_detected_list_does_not_exclude_any_row(
    tmp_db: Path, mocker: pytest.MockerFixture
) -> None:
    """With an empty detected list, all pending rows are returned regardless of booking_text."""
    mocker.patch(
        "swiss_exp_tracker.pipeline_agentic.pipeline.load_credit_card_payment_texts",
        return_value=[],
    )
    rfn_id = _insert_rfn(tmp_db, _make_rfn_row(booking_text="Ihre Zahlung - Danke"))

    result = load_pending_transactions()

    refined_ids = [tx.refined_id for tx in result]
    assert rfn_id in refined_ids


# ---------------------------------------------------------------------------
# Missing booking_text triage (regression)
# ---------------------------------------------------------------------------


def test_missing_booking_text_with_data_excluded_and_marked_needs_review(
    tmp_db: Path, mocker: pytest.MockerFixture
) -> None:
    """Row with no booking_text but carrying data is excluded from result and marked needs_review."""
    mocker.patch(
        "swiss_exp_tracker.pipeline_agentic.pipeline.load_credit_card_payment_texts",
        return_value=[],
    )
    rfn_id = _insert_rfn(
        tmp_db,
        _make_rfn_row(
            booking_text=None,
            date="2025-06-01",
            reference="REF-999",
            amount=50.0,
        ),
    )

    result = load_pending_transactions()

    refined_ids = [tx.refined_id for tx in result]
    assert rfn_id not in refined_ids
    assert _get_enrichment_status(tmp_db, rfn_id) == "needs_review"


def test_all_empty_row_with_no_booking_text_excluded_and_marked_skipped(
    tmp_db: Path, mocker: pytest.MockerFixture
) -> None:
    """Row with no booking_text and no other data (all-empty) is excluded and marked skipped."""
    mocker.patch(
        "swiss_exp_tracker.pipeline_agentic.pipeline.load_credit_card_payment_texts",
        return_value=[],
    )
    rfn_id = _insert_rfn(
        tmp_db,
        _make_rfn_row(
            booking_text=None,
            date=None,
            merchant_normalized=None,
            reference=None,
            amount=0.0,
        ),
    )

    result = load_pending_transactions()

    refined_ids = [tx.refined_id for tx in result]
    assert rfn_id not in refined_ids
    assert _get_enrichment_status(tmp_db, rfn_id) == "skipped"
