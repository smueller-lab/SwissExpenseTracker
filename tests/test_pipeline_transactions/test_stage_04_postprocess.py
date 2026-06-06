from __future__ import annotations

import sqlite3

from datetime import datetime
from pathlib import Path

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


def test_credit_card_pair_removed(tmp_db: Path) -> None:
    """Matched ZKB EXPENSE + VISECA INCOME of same amount → both deleted."""
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
