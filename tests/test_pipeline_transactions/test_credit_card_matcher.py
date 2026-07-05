"""Tests for find_credit_card_payment_pairs in credit_card_matcher.py."""

from __future__ import annotations

import sqlite3

from datetime import datetime
from pathlib import Path

import pytest

from swiss_exp_tracker.pipeline_ingestion.stages.transactions.credit_card_matcher import (
    find_credit_card_payment_pairs,
)

# ---------------------------------------------------------------------------
# DB seeding helper
# ---------------------------------------------------------------------------


def _insert_rfn(
    db_path: Path,
    source_type: str,
    amount: float,
    transaction_type: str,
    booking_text: str | None = None,
    date: str | None = "2025-01-01",
) -> int:
    """Insert full FK chain + one rfn row with a configurable date; return rfn.id."""
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
            "VALUES (?, ?, ?, ?, ?, ?, NULL, 0, 'CHF', ?, 'pending', ?)",
            (
                raw_id,
                source_type,
                date,
                amount,
                transaction_type,
                booking_text,
                f"REF-{raw_id}",
                datetime.now().isoformat(),
            ),
        )
        rfn_id = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
        db.commit()
    return rfn_id


# ---------------------------------------------------------------------------
# UBS_DEBIT ↔ UBS_CREDIT — happy path
# ---------------------------------------------------------------------------


def test_ubs_lsv_expense_and_credit_settlement_matches(tmp_db: Path) -> None:
    """UBS_DEBIT LSV expense + UBS_CREDIT empty-text INCOME → 1 match, is_lsv=True."""
    debit_id = _insert_rfn(
        tmp_db, "UBS_DEBIT", 100.0, "EXPENSE", "LSV payment", "2025-01-01"
    )
    credit_id = _insert_rfn(tmp_db, "UBS_CREDIT", 100.0, "INCOME", None, "2025-01-02")

    with sqlite3.connect(tmp_db) as db:
        matches = find_credit_card_payment_pairs(db)

    assert len(matches) == 1
    m = matches[0]
    assert m.debit_id == debit_id
    assert m.credit_id == credit_id
    assert m.debit_source == "UBS_DEBIT"
    assert m.credit_source == "UBS_CREDIT"
    assert m.amount == pytest.approx(100.0)
    assert m.is_lsv is True
    assert m.date == "2025-01-01"


# ---------------------------------------------------------------------------
# ZKB_DEBIT ↔ VISECA — happy path
# ---------------------------------------------------------------------------


def test_zkb_viseca_payment_expense_and_viseca_income_matches(tmp_db: Path) -> None:
    """ZKB_DEBIT 'Viseca Payment' expense + VISECA empty-text INCOME → 1 match, is_lsv=False."""
    debit_id = _insert_rfn(
        tmp_db, "ZKB_DEBIT", 200.0, "EXPENSE", "Viseca Payment", "2025-02-01"
    )
    credit_id = _insert_rfn(tmp_db, "VISECA", 200.0, "INCOME", None, "2025-02-03")

    with sqlite3.connect(tmp_db) as db:
        matches = find_credit_card_payment_pairs(db)

    assert len(matches) == 1
    m = matches[0]
    assert m.debit_id == debit_id
    assert m.credit_id == credit_id
    assert m.debit_source == "ZKB_DEBIT"
    assert m.credit_source == "VISECA"
    assert m.amount == pytest.approx(200.0)
    assert m.is_lsv is False


# ---------------------------------------------------------------------------
# 7-day window boundary
# ---------------------------------------------------------------------------


def test_within_7_days_boundary_matches_and_date_is_debit_date(tmp_db: Path) -> None:
    """Legs exactly 7 days apart match; match.date equals the debit leg's date."""
    debit_id = _insert_rfn(tmp_db, "UBS_DEBIT", 150.0, "EXPENSE", "LSV", "2025-03-01")
    credit_id = _insert_rfn(tmp_db, "UBS_CREDIT", 150.0, "INCOME", None, "2025-03-08")

    with sqlite3.connect(tmp_db) as db:
        matches = find_credit_card_payment_pairs(db)

    assert len(matches) == 1
    assert matches[0].debit_id == debit_id
    assert matches[0].credit_id == credit_id
    assert matches[0].date == "2025-03-01"


def test_beyond_7_days_no_match(tmp_db: Path) -> None:
    """Equal-amount legs 8 days apart do not match."""
    _insert_rfn(tmp_db, "UBS_DEBIT", 150.0, "EXPENSE", "LSV", "2025-03-01")
    _insert_rfn(tmp_db, "UBS_CREDIT", 150.0, "INCOME", None, "2025-03-09")

    with sqlite3.connect(tmp_db) as db:
        matches = find_credit_card_payment_pairs(db)

    assert matches == []


# ---------------------------------------------------------------------------
# Amount rounding
# ---------------------------------------------------------------------------


def test_amount_rounding_within_tolerance_matches(tmp_db: Path) -> None:
    """100.0 vs 100.004 round to the same 2-dp amount key and match."""
    _insert_rfn(tmp_db, "UBS_DEBIT", 100.0, "EXPENSE", "LSV", "2025-01-01")
    _insert_rfn(tmp_db, "UBS_CREDIT", 100.004, "INCOME", None, "2025-01-02")

    with sqlite3.connect(tmp_db) as db:
        matches = find_credit_card_payment_pairs(db)

    assert len(matches) == 1


def test_amount_rounding_outside_tolerance_no_match(tmp_db: Path) -> None:
    """100.0 vs 100.05 round to different 2-dp keys and do not match."""
    _insert_rfn(tmp_db, "UBS_DEBIT", 100.0, "EXPENSE", "LSV", "2025-01-01")
    _insert_rfn(tmp_db, "UBS_CREDIT", 100.05, "INCOME", None, "2025-01-02")

    with sqlite3.connect(tmp_db) as db:
        matches = find_credit_card_payment_pairs(db)

    assert matches == []


# ---------------------------------------------------------------------------
# No double-match — one debit, two equal credits
# ---------------------------------------------------------------------------


def test_no_double_match_one_debit_two_equal_credits(tmp_db: Path) -> None:
    """One debit, two equal credit rows → exactly 1 pair; nearest credit is consumed."""
    debit_id = _insert_rfn(tmp_db, "UBS_DEBIT", 100.0, "EXPENSE", "LSV", "2025-01-05")
    credit_id_near = _insert_rfn(
        tmp_db, "UBS_CREDIT", 100.0, "INCOME", None, "2025-01-06"
    )
    _credit_id_far = _insert_rfn(
        tmp_db, "UBS_CREDIT", 100.0, "INCOME", None, "2025-01-08"
    )

    with sqlite3.connect(tmp_db) as db:
        matches = find_credit_card_payment_pairs(db)

    assert len(matches) == 1
    assert matches[0].debit_id == debit_id
    # The nearer credit leg (1 day away) must be chosen over the farther one (3 days).
    assert matches[0].credit_id == credit_id_near


# ---------------------------------------------------------------------------
# Unparseable date — no match
# ---------------------------------------------------------------------------


def test_no_parseable_date_debit_no_match(tmp_db: Path) -> None:
    """A debit leg with NULL date cannot satisfy the 7-day window — yields no match."""
    _insert_rfn(tmp_db, "UBS_DEBIT", 100.0, "EXPENSE", "LSV", None)
    _insert_rfn(tmp_db, "UBS_CREDIT", 100.0, "INCOME", None, "2025-01-02")

    with sqlite3.connect(tmp_db) as db:
        matches = find_credit_card_payment_pairs(db)

    assert matches == []


# ---------------------------------------------------------------------------
# enrichment_status='pending' on credit leg — matching is status-independent
# ---------------------------------------------------------------------------


def test_credit_leg_with_pending_enrichment_status_matches(tmp_db: Path) -> None:
    """Matching is driven by source/type/amount/date only — enrichment_status='pending' on credit leg does not block pairing.

    This regression guards against a future change to the SQL query accidentally
    filtering by enrichment_status; a detected card-payment row left pending by the
    agentic pipeline must still be paired by the credit-card matcher.
    """
    debit_id = _insert_rfn(
        tmp_db, "ZKB_DEBIT", 150.0, "EXPENSE", "Viseca Payment", "2025-05-01"
    )
    credit_id = _insert_rfn(tmp_db, "VISECA", 150.0, "INCOME", None, "2025-05-03")

    # Confirm the credit leg carries enrichment_status='pending' (as left by the agentic
    # pipeline when it skips detected card-payment rows without enriching them).
    with sqlite3.connect(tmp_db) as db:
        row = db.execute(
            "SELECT enrichment_status FROM transactions_rfn WHERE id = ?", (credit_id,)
        ).fetchone()
    assert row is not None
    assert row[0] == "pending"

    with sqlite3.connect(tmp_db) as db:
        matches = find_credit_card_payment_pairs(db)

    assert len(matches) == 1
    assert matches[0].debit_id == debit_id
    assert matches[0].credit_id == credit_id
    assert matches[0].amount == pytest.approx(150.0)
