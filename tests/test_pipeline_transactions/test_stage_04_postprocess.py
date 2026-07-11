from __future__ import annotations

import sqlite3

from datetime import datetime
from pathlib import Path

import pytest
import yaml

import swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_04_postprocess as _postprocess_mod

from swiss_exp_tracker.config.user_config_schema import DetectedRules
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_04_postprocess import (
    _record_credit_card_payments,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_04_postprocess import (
    run_postprocess,
)

# ---------------------------------------------------------------------------
# DB seeding helper
# ---------------------------------------------------------------------------


def _insert_rfn_row(
    db_path: Path,
    source_type: str,
    amount: float,
    transaction_type: str,
    booking_text: str | None = None,
    merchant_normalized: str | None = None,
) -> int:
    """Insert a full FK chain and one rfn row; return rfn.id."""
    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT OR IGNORE INTO ingested_files "
            "(filename, file_hash, source_type, ingested_at, record_count, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                "pp_test.csv",
                "hash1",
                source_type,
                datetime.now().isoformat(),
                1,
                "refined",
            ),
        )
        file_id = db.execute(
            "SELECT id FROM ingested_files WHERE filename='pp_test.csv' AND source_type=? "
            "ORDER BY id DESC LIMIT 1",
            (source_type,),
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
            (lnd_id, source_type, "{}", "pp_test.csv", datetime.now().isoformat()),
        )
        raw_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

        db.execute(
            "INSERT INTO transactions_rfn "
            "(raw_id, source_type, date, amount, transaction_type, "
            " booking_text, merchant_normalized, is_person, currency, "
            " reference, enrichment_status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
            (
                raw_id,
                source_type,
                "2025-01-01",
                amount,
                transaction_type,
                booking_text,
                merchant_normalized,
                "CHF",
                f"REF-{raw_id}",
                "pending",
                datetime.now().isoformat(),
            ),
        )
        rfn_id = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
        db.commit()

    return rfn_id


# ---------------------------------------------------------------------------
# Credit card pair removal
# ---------------------------------------------------------------------------


def test_credit_card_pair_removed(
    tmp_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Matched ZKB EXPENSE + VISECA INCOME of same amount → both deleted."""
    rules_path = tmp_path / "detected_rules.yaml"
    monkeypatch.setattr(
        _postprocess_mod,
        "_record_credit_card_payments",
        lambda matches: _record_credit_card_payments(matches, output_path=rules_path),
    )

    _insert_rfn_row(
        tmp_db,
        source_type="ZKB_DEBIT",
        amount=50.0,
        transaction_type="EXPENSE",
        booking_text="Viseca Payment",
    )
    _insert_rfn_row(
        tmp_db,
        source_type="VISECA",
        amount=50.0,
        transaction_type="INCOME",
        booking_text=None,
    )

    result = run_postprocess()

    assert result["credit_card_pairs_removed"] == 1

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert rfn_count == 0


def test_no_matching_pair_untouched(tmp_db: Path) -> None:
    """ZKB EXPENSE with no matching VISECA INCOME → row kept, pairs_removed=0."""
    _insert_rfn_row(
        tmp_db,
        source_type="ZKB_DEBIT",
        amount=50.0,
        transaction_type="EXPENSE",
        booking_text="Viseca Payment",
    )

    result = run_postprocess()

    assert result["credit_card_pairs_removed"] == 0

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert rfn_count == 1


# ---------------------------------------------------------------------------
# Viseca fee text backfill
# ---------------------------------------------------------------------------


def test_viseca_fee_text_filled(tmp_db: Path) -> None:
    """VISECA EXPENSE of amount=2.0 with no text → booking_text and merchant filled."""
    _insert_rfn_row(
        tmp_db,
        source_type="VISECA",
        amount=2.0,
        transaction_type="EXPENSE",
        booking_text=None,
        merchant_normalized=None,
    )

    run_postprocess()

    with sqlite3.connect(tmp_db) as db:
        row = db.execute(
            "SELECT booking_text, merchant_normalized FROM transactions_rfn LIMIT 1"
        ).fetchone()

    assert row[0] == "Credit card fee"
    assert row[1] == "credit card fee"


def test_viseca_fee_only_fills_correct_amount(tmp_db: Path) -> None:
    """VISECA EXPENSE of amount=10.0 (not 2.0) → booking_text left unchanged (None)."""
    _insert_rfn_row(
        tmp_db,
        source_type="VISECA",
        amount=10.0,
        transaction_type="EXPENSE",
        booking_text=None,
        merchant_normalized=None,
    )

    run_postprocess()

    with sqlite3.connect(tmp_db) as db:
        booking_text = db.execute(
            "SELECT booking_text FROM transactions_rfn LIMIT 1"
        ).fetchone()[0]

    assert booking_text is None


# ---------------------------------------------------------------------------
# Empty DB
# ---------------------------------------------------------------------------


def test_empty_db_returns_zero_counters(tmp_db: Path) -> None:
    """Empty rfn table → all postprocess counters are 0."""
    result = run_postprocess()

    assert result["credit_card_pairs_removed"] == 0
    assert result["viseca_fee_rows_updated"] == 0


# ---------------------------------------------------------------------------
# Postprocess deletion — UBS pair, unmatched row survives
# ---------------------------------------------------------------------------


def test_postprocess_matched_legs_deleted_unmatched_remains(
    tmp_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """UBS pair + unmatched ZKB row: matched legs deleted, unmatched row remains, counter=1."""
    rules_path = tmp_path / "detected_rules.yaml"
    # Redirect _record_credit_card_payments to a tmp file so the real config is never touched.
    monkeypatch.setattr(
        _postprocess_mod,
        "_record_credit_card_payments",
        lambda matches: _record_credit_card_payments(matches, output_path=rules_path),
    )

    # Unmatched row: booking_text does not match the LSV/Viseca SQL filter.
    _insert_rfn_row(
        tmp_db,
        source_type="ZKB_DEBIT",
        amount=80.0,
        transaction_type="EXPENSE",
        booking_text="Regular purchase",
    )
    # Matched pair: UBS_DEBIT LSV expense + UBS_CREDIT empty-text settlement.
    _insert_rfn_row(
        tmp_db,
        source_type="UBS_DEBIT",
        amount=75.0,
        transaction_type="EXPENSE",
        booking_text="LSV payment",
    )
    _insert_rfn_row(
        tmp_db,
        source_type="UBS_CREDIT",
        amount=75.0,
        transaction_type="INCOME",
        booking_text=None,
    )

    result = run_postprocess()

    assert result["credit_card_pairs_removed"] == 1

    with sqlite3.connect(tmp_db) as db:
        remaining = db.execute(
            "SELECT source_type, booking_text FROM transactions_rfn"
        ).fetchall()

    assert len(remaining) == 1
    assert remaining[0][0] == "ZKB_DEBIT"
    assert remaining[0][1] == "Regular purchase"


# ---------------------------------------------------------------------------
# Postprocess persistence — YAML written with credit_card_payments section
# ---------------------------------------------------------------------------


def test_postprocess_persistence_yaml_contains_pairs(
    tmp_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After run_postprocess the tmp detected_rules.yaml holds the matched pair."""
    rules_path = tmp_path / "detected_rules.yaml"
    monkeypatch.setattr(
        _postprocess_mod,
        "_record_credit_card_payments",
        lambda matches: _record_credit_card_payments(matches, output_path=rules_path),
    )

    _insert_rfn_row(
        tmp_db,
        source_type="ZKB_DEBIT",
        amount=50.0,
        transaction_type="EXPENSE",
        booking_text="Viseca Payment",
    )
    _insert_rfn_row(
        tmp_db,
        source_type="VISECA",
        amount=50.0,
        transaction_type="INCOME",
        booking_text="2002 LSV-Zahlung",
    )

    run_postprocess()

    assert rules_path.exists()
    with rules_path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    parsed = DetectedRules.model_validate(data)

    # Both legs' distinct texts are recorded, sorted and deduplicated.
    assert parsed.credit_card_payments.booking_texts == [
        "2002 LSV-Zahlung",
        "Viseca Payment",
    ]


# ---------------------------------------------------------------------------
# Revolut transfer pair removal
# ---------------------------------------------------------------------------


def test_zkb_revolut_topup_removed_and_counter_correct(tmp_db: Path) -> None:
    """ZKB Debit EXPENSE with 'revolut' in merchant → dropped; revolut_transfers_removed counts it."""
    _insert_rfn_row(
        tmp_db,
        source_type="ZKB_DEBIT",
        amount=1000.0,
        transaction_type="EXPENSE",
        booking_text="Debit eBanking: Revolut LTD, CH",
        merchant_normalized="revolut ch",
    )

    result = run_postprocess()

    assert result["revolut_transfers_removed"] == 1

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert rfn_count == 0


def test_revolut_source_expense_is_not_dropped(tmp_db: Path) -> None:
    """A REVOLUT-source EXPENSE ('To David') is never dropped by the ZKB top-up cleanup."""
    _insert_rfn_row(
        tmp_db,
        source_type="REVOLUT",
        amount=980.00,
        transaction_type="EXPENSE",
        booking_text="To David Uso",
        merchant_normalized="to david uso",
    )

    result = run_postprocess()

    assert result["revolut_transfers_removed"] == 0

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert rfn_count == 1


def test_zkb_non_revolut_expense_is_kept(tmp_db: Path) -> None:
    """A ZKB Debit EXPENSE whose merchant is not Revolut is kept; counter stays 0."""
    _insert_rfn_row(
        tmp_db,
        source_type="ZKB_DEBIT",
        amount=42.0,
        transaction_type="EXPENSE",
        booking_text="Migros",
        merchant_normalized="migros",
    )

    result = run_postprocess()

    assert result["revolut_transfers_removed"] == 0

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert rfn_count == 1


def test_postprocess_result_dict_contains_revolut_transfers_removed(
    tmp_db: Path,
) -> None:
    """run_postprocess() always returns the 'revolut_transfers_removed' key, even with empty DB."""
    result = run_postprocess()

    assert "revolut_transfers_removed" in result
    assert result["revolut_transfers_removed"] == 0


def test_revolut_topup_and_credit_card_both_cleaned(
    tmp_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ZKB Revolut top-up + credit-card pair both cleaned independently; counters correct."""
    rules_path = tmp_path / "detected_rules.yaml"
    monkeypatch.setattr(
        _postprocess_mod,
        "_record_credit_card_payments",
        lambda matches: _record_credit_card_payments(matches, output_path=rules_path),
    )

    # ZKB Revolut top-up (dropped by merchant substring).
    _insert_rfn_row(
        tmp_db,
        source_type="ZKB_DEBIT",
        amount=1000.0,
        transaction_type="EXPENSE",
        booking_text="Debit eBanking: Revolut LTD, CH",
        merchant_normalized="revolut ch",
    )
    # Credit card pair: ZKB_DEBIT Viseca + VISECA INCOME (matched by amount).
    _insert_rfn_row(
        tmp_db,
        source_type="ZKB_DEBIT",
        amount=50.0,
        transaction_type="EXPENSE",
        booking_text="Viseca Payment",
    )
    _insert_rfn_row(
        tmp_db,
        source_type="VISECA",
        amount=50.0,
        transaction_type="INCOME",
        booking_text=None,
    )

    result = run_postprocess()

    assert result["revolut_transfers_removed"] == 1
    assert result["credit_card_pairs_removed"] == 1

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert rfn_count == 0
