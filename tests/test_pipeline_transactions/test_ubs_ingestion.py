from __future__ import annotations

import shutil
import sqlite3

from pathlib import Path

from swiss_exp_tracker.pipeline_ingestion.data_models.data_sources import get_profile
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_01_landing import (
    _read_rows,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_01_landing import (
    process_landing_source,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_02_raw import (
    process_raw_source,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_03_refined import (
    process_refined_source,
)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"


def _expected_row_count(file_name: str, source_type: SourceType) -> int:
    """Number of data rows the UBS reader finds in a fixture (derived, not hardcoded)."""
    return len(_read_rows(TEST_DATA_DIR / file_name, get_profile(source_type)))


def _run_ingestion(folder: Path, source_type: SourceType) -> None:
    """Run landing → raw → refined for a single source against the temp DB."""
    process_landing_source(folder, source_type)
    process_raw_source(source_type)
    process_refined_source(source_type)


def test_ubs_debit_lands_and_refines(tmp_path: Path, tmp_db: Path) -> None:
    """UBS debit fixture flows landing→refined as CHF expenses pending enrichment."""
    expected = _expected_row_count("ubs_debit_test.csv", SourceType.UBS_DEBIT)
    folder = tmp_path / "ubs_debit"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "ubs_debit_test.csv", folder)

    result = process_landing_source(folder, SourceType.UBS_DEBIT)
    assert result["records_inserted"] == expected

    process_raw_source(SourceType.UBS_DEBIT)
    process_refined_source(SourceType.UBS_DEBIT)

    with sqlite3.connect(tmp_db) as db:
        rows = db.execute(
            "SELECT amount, transaction_type, currency, enrichment_status, balance_chf "
            "FROM transactions_rfn WHERE source_type='UBS_DEBIT'"
        ).fetchall()

    assert len(rows) == expected
    assert all(amount > 0 for amount, _, _, _, _ in rows)
    assert all(ttype == "EXPENSE" for _, ttype, _, _, _ in rows)
    assert all(currency == "CHF" for _, _, currency, _, _ in rows)
    assert all(status == "pending" for _, _, _, status, _ in rows)
    # Solde column → running balance is populated for every debit row.
    assert all(balance is not None for _, _, _, _, balance in rows)


def test_ubs_credit_lands_and_refines(tmp_path: Path, tmp_db: Path) -> None:
    """UBS credit fixture flows landing→refined as positive-amount pending records."""
    expected = _expected_row_count("ubs_credit_test.csv", SourceType.UBS_CREDIT)
    folder = tmp_path / "ubs_credit"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "ubs_credit_test.csv", folder)

    result = process_landing_source(folder, SourceType.UBS_CREDIT)
    assert result["records_inserted"] == expected

    process_raw_source(SourceType.UBS_CREDIT)
    process_refined_source(SourceType.UBS_CREDIT)

    with sqlite3.connect(tmp_db) as db:
        rows = db.execute(
            "SELECT amount, transaction_type, enrichment_status "
            "FROM transactions_rfn WHERE source_type='UBS_CREDIT'"
        ).fetchall()

    assert len(rows) == expected
    assert all(amount > 0 for amount, _, _ in rows)
    assert all(ttype in ("EXPENSE", "INCOME") for _, ttype, _ in rows)
    assert all(status == "pending" for _, _, status in rows)


def test_ubs_debit_idempotent_reingest(tmp_path: Path, tmp_db: Path) -> None:
    """Re-running landing on the same UBS debit file inserts no new rows."""
    expected = _expected_row_count("ubs_debit_test.csv", SourceType.UBS_DEBIT)
    folder = tmp_path / "ubs_debit"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "ubs_debit_test.csv", folder)

    _run_ingestion(folder, SourceType.UBS_DEBIT)
    result2 = process_landing_source(folder, SourceType.UBS_DEBIT)

    assert result2["files_found"] == 0
    assert result2["records_inserted"] == 0

    with sqlite3.connect(tmp_db) as db:
        rfn_count = db.execute(
            "SELECT COUNT(*) FROM transactions_rfn WHERE source_type='UBS_DEBIT'"
        ).fetchone()[0]
    assert rfn_count == expected
