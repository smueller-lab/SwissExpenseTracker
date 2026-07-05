from __future__ import annotations

import csv
import json
import sqlite3

from datetime import datetime
from pathlib import Path

import pytest

from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_03_refined import (
    process_refined_source,
)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"

# ---------------------------------------------------------------------------
# DB seeding helpers
# ---------------------------------------------------------------------------


def _seed_raw_row(
    db_path: Path,
    raw_json: str,
    source_type: str = "ZKB_DEBIT",
    filename: str = "test.csv",
) -> int:
    """Insert full chain ingested_files → lnd → raw; return raw_id.

    Uses INSERT OR IGNORE for ingested_files to allow multiple rows per file.
    """
    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT OR IGNORE INTO ingested_files "
            "(filename, file_hash, source_type, ingested_at, record_count, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (filename, "abc123", source_type, datetime.now().isoformat(), 1, "raw"),
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
            (file_id, source_type, raw_json, datetime.now().isoformat()),
        )
        lnd_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

        db.execute(
            "INSERT INTO transactions_raw "
            "(landing_id, source_type, raw_json, source_file, created_at, processed) "
            "VALUES (?, ?, ?, ?, ?, 0)",
            (lnd_id, source_type, raw_json, filename, datetime.now().isoformat()),
        )
        raw_id = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
        db.commit()

    return raw_id


# ---------------------------------------------------------------------------
# Row JSON helpers — build raw canonical-keyed row dicts
# ---------------------------------------------------------------------------


def _zkb_row_json(**overrides: object) -> str:
    """Return a canonical ZKB row JSON string, optionally overriding fields."""
    with (TEST_DATA_DIR / "zkb_test.csv").open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        row: dict[str, object] = dict(next(reader))
    row.update(overrides)
    return json.dumps(row, ensure_ascii=False)


def _viseca_row_json(**overrides: object) -> str:
    """Return a canonical Viseca row JSON string, optionally overriding fields."""
    with (TEST_DATA_DIR / "viseca_test.csv").open(
        encoding="utf-8-sig", newline=""
    ) as fh:
        reader = csv.DictReader(fh)
        row: dict[str, object] = dict(next(reader))
    row.update(overrides)
    return json.dumps(row, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


def test_happy_path_zkb_debit(tmp_db: Path) -> None:
    """One ZKB raw row → records_inserted=1, rfn.amount > 0, enrichment_status=pending."""
    raw_json = _zkb_row_json()
    _seed_raw_row(tmp_db, raw_json, "ZKB_DEBIT")

    result = process_refined_source(SourceType.ZKB_DEBIT)

    assert result["records_inserted"] == 1

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
        row = db.execute(
            "SELECT amount, enrichment_status FROM transactions_rfn LIMIT 1"
        ).fetchone()
        raw_processed = db.execute(
            "SELECT processed FROM transactions_raw LIMIT 1"
        ).fetchone()[0]

    assert rfn_count == 1
    assert row[0] > 0
    assert row[1] == "pending"
    assert raw_processed == 1


def test_happy_path_viseca(tmp_db: Path) -> None:
    """One Viseca raw row → records_inserted=1."""
    raw_json = _viseca_row_json()
    _seed_raw_row(tmp_db, raw_json, "VISECA")

    result = process_refined_source(SourceType.VISECA)

    assert result["records_inserted"] == 1


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_idempotency(tmp_db: Path) -> None:
    """Second run on already-processed rows returns records_inserted=0, rfn count=1."""
    raw_json = _zkb_row_json()
    _seed_raw_row(tmp_db, raw_json, "ZKB_DEBIT")

    process_refined_source(SourceType.ZKB_DEBIT)
    result2 = process_refined_source(SourceType.ZKB_DEBIT)

    assert result2["records_inserted"] == 0

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert rfn_count == 1


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def test_duplicate_skipped(tmp_db: Path) -> None:
    """Two raw rows with identical reference+date+amount → one rfn row, one duplicate."""
    raw_json = _zkb_row_json()
    _seed_raw_row(tmp_db, raw_json, "ZKB_DEBIT", filename="file1.csv")
    _seed_raw_row(tmp_db, raw_json, "ZKB_DEBIT", filename="file2.csv")

    result = process_refined_source(SourceType.ZKB_DEBIT)

    assert result["records_inserted"] == 1
    assert result["duplicates_found"] == 1

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert rfn_count == 1


# ---------------------------------------------------------------------------
# ZKB eBanking parent/detail row handling
# ---------------------------------------------------------------------------


def test_zkb_ebanking_parent_row_skipped(tmp_db: Path) -> None:
    """ZKB eBanking parent row (no date, no amounts) is skipped from rfn."""
    parent_json = json.dumps(
        {
            "Date": None,
            "Booking text": "Debit eBanking (1)",
            "Curr": None,
            "Amount details": None,
            "ZKB reference": "L99001",
            "Reference number": None,
            "Debit CHF": None,
            "Credit CHF": None,
            "Value date": None,
            "Balance CHF": None,
            "Payment purpose": None,
            "Details": None,
        }
    )
    _seed_raw_row(tmp_db, parent_json, "ZKB_DEBIT")

    result = process_refined_source(SourceType.ZKB_DEBIT)

    assert result["records_inserted"] == 0

    with sqlite3.connect(tmp_db) as db:
        raw_processed = db.execute(
            "SELECT processed FROM transactions_raw LIMIT 1"
        ).fetchone()[0]
    assert raw_processed == 1


def test_zkb_ebanking_detail_row_inserted(tmp_db: Path) -> None:
    """Rows from the eBanking test CSV are inserted and have non-null dates."""
    with (TEST_DATA_DIR / "zkb_ebanking_test.csv").open(
        encoding="utf-8-sig", newline=""
    ) as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    # Row 0 = parent (skipped), rows 1 and 2 are normal rows with their own dates.
    for row in rows:
        raw_json = json.dumps(dict(row), ensure_ascii=False)
        _seed_raw_row(tmp_db, raw_json, "ZKB_DEBIT")

    result = process_refined_source(SourceType.ZKB_DEBIT)

    # Parent row skipped → 2 normal rows inserted
    assert result["records_inserted"] == 2

    with sqlite3.connect(tmp_db) as db:
        dates = db.execute(
            "SELECT date FROM transactions_rfn WHERE source_type='ZKB_DEBIT'"
        ).fetchall()

    for (date_val,) in dates:
        assert date_val is not None


def test_zkb_mobile_banking_parent_row_skipped(tmp_db: Path) -> None:
    """'Debit Mobile Banking' without a count suffix is dropped as a master row."""
    parent_json = json.dumps(
        {
            "Date": None,
            "Booking text": "Debit Mobile Banking",
            "Curr": None,
            "Amount details": None,
            "ZKB reference": "L99002",
            "Reference number": None,
            "Debit CHF": None,
            "Credit CHF": None,
            "Value date": "2024-03-15",
            "Balance CHF": None,
            "Payment purpose": None,
            "Details": None,
        }
    )
    _seed_raw_row(tmp_db, parent_json, "ZKB_DEBIT")

    result = process_refined_source(SourceType.ZKB_DEBIT)

    assert result["records_inserted"] == 0

    with sqlite3.connect(tmp_db) as db:
        raw_processed = db.execute(
            "SELECT processed FROM transactions_raw LIMIT 1"
        ).fetchone()[0]
    assert raw_processed == 1


def test_zkb_mobile_banking_detail_rows_kept(tmp_db: Path) -> None:
    """Detail rows following a 'Debit Mobile Banking' parent are inserted into rfn."""
    parent_json = json.dumps(
        {
            "Date": None,
            "Booking text": "Debit Mobile Banking",
            "Curr": None,
            "Amount details": None,
            "ZKB reference": "L99003",
            "Reference number": None,
            "Debit CHF": None,
            "Credit CHF": None,
            "Value date": "2024-03-15",
            "Balance CHF": None,
            "Payment purpose": None,
            "Details": None,
        }
    )
    detail_json = json.dumps(
        {
            "Date": None,
            "Booking text": "Freshmart",
            "Curr": "CHF",
            "Amount details": "25.40",
            "ZKB reference": None,
            "Reference number": None,
            "Debit CHF": None,
            "Credit CHF": None,
            "Value date": None,
            "Balance CHF": None,
            "Payment purpose": None,
            "Details": None,
        }
    )
    _seed_raw_row(tmp_db, parent_json, "ZKB_DEBIT")
    _seed_raw_row(tmp_db, detail_json, "ZKB_DEBIT")

    result = process_refined_source(SourceType.ZKB_DEBIT)

    assert result["records_inserted"] == 1

    with sqlite3.connect(tmp_db) as db:
        date_val = db.execute(
            "SELECT date FROM transactions_rfn WHERE source_type='ZKB_DEBIT'"
        ).fetchone()[0]
    assert date_val is not None


# ---------------------------------------------------------------------------
# Revolut skip conditions
# ---------------------------------------------------------------------------


def test_revolut_exchange_leg_skipped(tmp_db: Path) -> None:
    """Matched Revolut exchange pair → both rows skipped, records_inserted=0."""
    started = "2023-06-01 10:00:00"
    chf_row = {
        "Type": "Exchange",
        "Product": "Current",
        "Started Date": started,
        "Completed Date": started,
        "Description": "Exchange to EUR",
        "Amount": "-100.0",
        "Fee": "0",
        "Currency": "CHF",
        "State": "COMPLETED",
        "Balance": "900",
    }
    eur_row = {
        "Type": "Exchange",
        "Product": "Current",
        "Started Date": started,
        "Completed Date": started,
        "Description": "Exchange to EUR",
        "Amount": "104.0",
        "Fee": "0",
        "Currency": "EUR",
        "State": "COMPLETED",
        "Balance": "104",
    }
    for row_dict in (chf_row, eur_row):
        raw_json = json.dumps(row_dict, ensure_ascii=False)
        _seed_raw_row(tmp_db, raw_json, "REVOLUT")

    result = process_refined_source(SourceType.REVOLUT)

    assert result["records_inserted"] == 0

    with sqlite3.connect(tmp_db) as db:
        processed_count = db.execute(
            "SELECT COUNT(*) FROM transactions_raw WHERE processed=1"
        ).fetchone()[0]
    assert processed_count == 2


def test_revolut_topup_skipped(tmp_db: Path) -> None:
    """Revolut topup row is skipped and marked processed."""
    topup_row = {
        "Type": "topup",
        "Product": "Current",
        "Started Date": "2023-06-01 10:00:00",
        "Completed Date": "2023-06-01 10:00:00",
        "Description": "Top-Up by *1234",
        "Amount": "200.0",
        "Fee": "0",
        "Currency": "CHF",
        "State": "COMPLETED",
        "Balance": "300",
    }
    raw_json = json.dumps(topup_row, ensure_ascii=False)
    _seed_raw_row(tmp_db, raw_json, "REVOLUT")

    result = process_refined_source(SourceType.REVOLUT)

    assert result["records_inserted"] == 0

    with sqlite3.connect(tmp_db) as db:
        processed = db.execute(
            "SELECT processed FROM transactions_raw LIMIT 1"
        ).fetchone()[0]
    assert processed == 1


# ---------------------------------------------------------------------------
# Amount invariant
# ---------------------------------------------------------------------------


def test_amount_is_absolute(tmp_db: Path) -> None:
    """ZKB debit amount is stored as absolute value in rfn."""
    raw_json = _zkb_row_json()
    _seed_raw_row(tmp_db, raw_json, "ZKB_DEBIT")

    process_refined_source(SourceType.ZKB_DEBIT)

    with sqlite3.connect(tmp_db) as db:
        amount = db.execute("SELECT amount FROM transactions_rfn LIMIT 1").fetchone()[0]
    assert amount >= 0


# ---------------------------------------------------------------------------
# File status promotion
# ---------------------------------------------------------------------------


def test_file_status_promoted_to_refined(tmp_db: Path) -> None:
    """File status advances to 'refined' after all raw rows for the file are processed."""
    raw_json = _zkb_row_json()
    _seed_raw_row(tmp_db, raw_json, "ZKB_DEBIT", filename="promo_test.csv")

    process_refined_source(SourceType.ZKB_DEBIT)

    with sqlite3.connect(tmp_db) as db:
        status = db.execute(
            "SELECT status FROM ingested_files WHERE filename='promo_test.csv'"
        ).fetchone()[0]
    assert status == "refined"


# ---------------------------------------------------------------------------
# Unprocessed-row filtering
# ---------------------------------------------------------------------------


def test_only_unprocessed_raw_rows_refined(tmp_db: Path) -> None:
    """Pre-marked raw rows are skipped; rows_found counts only unprocessed rows."""
    raw_id_1 = _seed_raw_row(
        tmp_db, _zkb_row_json(**{"ZKB reference": "L10001"}), "ZKB_DEBIT"
    )
    _seed_raw_row(tmp_db, _zkb_row_json(**{"ZKB reference": "L10002"}), "ZKB_DEBIT")
    _seed_raw_row(tmp_db, _zkb_row_json(**{"ZKB reference": "L10003"}), "ZKB_DEBIT")

    with sqlite3.connect(tmp_db) as db:
        db.execute("UPDATE transactions_raw SET processed=1 WHERE id=?", (raw_id_1,))
        db.commit()

    result = process_refined_source(SourceType.ZKB_DEBIT)

    assert result["rows_found"] == 2

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert rfn_count == 2


# ---------------------------------------------------------------------------
# Source-type isolation
# ---------------------------------------------------------------------------


def test_source_type_isolation_stage03(tmp_db: Path) -> None:
    """process_refined_source(ZKB_DEBIT) leaves VISECA raw rows unprocessed."""
    zkb_json = _zkb_row_json()
    vis_json = _viseca_row_json()

    _seed_raw_row(tmp_db, zkb_json, "ZKB_DEBIT")
    vis_raw_id = _seed_raw_row(tmp_db, vis_json, "VISECA")

    process_refined_source(SourceType.ZKB_DEBIT)

    with sqlite3.connect(tmp_db) as db:
        zkb_processed = db.execute(
            "SELECT processed FROM transactions_raw WHERE source_type='ZKB_DEBIT'"
        ).fetchone()[0]
        vis_processed = db.execute(
            "SELECT processed FROM transactions_raw WHERE id=?", (vis_raw_id,)
        ).fetchone()[0]
        rfn_sources = {
            row[0]
            for row in db.execute(
                "SELECT DISTINCT source_type FROM transactions_rfn"
            ).fetchall()
        }

    assert zkb_processed == 1
    assert vis_processed == 0
    assert "VISECA" not in rfn_sources


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_null_date_row_skipped_as_pending(tmp_db: Path) -> None:
    """Row with no date is pending → skipped, not inserted, and marked processed."""
    raw_json = _zkb_row_json(**{"Date": "", "Value date": ""})
    _seed_raw_row(tmp_db, raw_json, "ZKB_DEBIT")

    result = process_refined_source(SourceType.ZKB_DEBIT)

    assert result["records_inserted"] == 0
    assert result["pending_skipped"] == 1
    assert result["rows_processed"] == 1

    with sqlite3.connect(tmp_db) as db:
        count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]
    assert count == 0


def test_zkb_all_amounts_none_produces_zero(tmp_db: Path) -> None:
    """ZKB row with all amount fields empty → inserted with amount=0.0."""
    raw_json = _zkb_row_json(
        **{"Debit CHF": "", "Credit CHF": "", "Amount details": "", "Curr": ""}
    )
    _seed_raw_row(tmp_db, raw_json, "ZKB_DEBIT")

    result = process_refined_source(SourceType.ZKB_DEBIT)

    assert result["records_inserted"] == 1

    with sqlite3.connect(tmp_db) as db:
        amount = db.execute("SELECT amount FROM transactions_rfn LIMIT 1").fetchone()[0]
    assert amount == pytest.approx(0.0)


def test_zkb_missing_reference_generates_noid(tmp_db: Path) -> None:
    """ZKB row with empty reference fields → rfn.reference starts with NOID-."""
    raw_json = _zkb_row_json(**{"ZKB reference": "", "Reference number": ""})
    _seed_raw_row(tmp_db, raw_json, "ZKB_DEBIT")

    process_refined_source(SourceType.ZKB_DEBIT)

    with sqlite3.connect(tmp_db) as db:
        reference = db.execute(
            "SELECT reference FROM transactions_rfn LIMIT 1"
        ).fetchone()[0]
    assert reference.startswith("NOID-")


def test_revolut_unknown_currency_raises(tmp_db: Path) -> None:
    """Revolut row with unknown currency raises ValueError during refinement."""
    invalid_row = {
        "Type": "Payment",
        "Product": "Current",
        "Started Date": "2023-06-01 10:00:00",
        "Completed Date": "2023-06-01 10:00:00",
        "Description": "Test",
        "Amount": "-10.0",
        "Fee": "0",
        "Currency": "XYZ",
        "State": "COMPLETED",
        "Balance": "90",
    }
    raw_json = json.dumps(invalid_row, ensure_ascii=False)
    _seed_raw_row(tmp_db, raw_json, "REVOLUT")

    with pytest.raises(ValueError):
        process_refined_source(SourceType.REVOLUT)


def test_viseca_null_merchant_does_not_crash(tmp_db: Path) -> None:
    """Viseca row with empty MerchantName → inserted without crash."""
    raw_json = _viseca_row_json(MerchantName="")
    _seed_raw_row(tmp_db, raw_json, "VISECA")

    result = process_refined_source(SourceType.VISECA)

    assert result["records_inserted"] == 1


def test_revolut_completed_date_null_uses_started_date(tmp_db: Path) -> None:
    """Revolut row with empty Completed Date uses Started Date for rfn.date."""
    payment_row = {
        "Type": "Payment",
        "Product": "Current",
        "Started Date": "2023-01-09 13:44:17",
        "Completed Date": "",
        "Description": "Test Store",
        "Amount": "-15.0",
        "Fee": "0",
        "Currency": "CHF",
        "State": "COMPLETED",
        "Balance": "85",
    }
    raw_json = json.dumps(payment_row, ensure_ascii=False)
    _seed_raw_row(tmp_db, raw_json, "REVOLUT")

    process_refined_source(SourceType.REVOLUT)

    with sqlite3.connect(tmp_db) as db:
        date_val = db.execute("SELECT date FROM transactions_rfn LIMIT 1").fetchone()[0]

    assert date_val is not None
    assert "2023-01-09" in date_val
