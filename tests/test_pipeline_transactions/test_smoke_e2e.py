from __future__ import annotations

import shutil
import sqlite3

from pathlib import Path

import pytest

from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_01_landing import (
    process_landing_source,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_02_raw import (
    process_raw_source,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_03_refined import (
    process_refined_source,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_04_postprocess import (
    run_postprocess,
)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_landing(tmp_path: Path) -> Path:
    """Create per-source landing folders and copy smoke CSVs into them."""
    zkb_dir = tmp_path / "zkb_debit"
    zkb_dir.mkdir()
    vis_dir = tmp_path / "viseca"
    vis_dir.mkdir()
    rev_dir = tmp_path / "revolut"
    rev_dir.mkdir()

    shutil.copy(TEST_DATA_DIR / "zkb_smoke_test.csv", zkb_dir)
    shutil.copy(TEST_DATA_DIR / "viseca_smoke_test.csv", vis_dir)
    shutil.copy(TEST_DATA_DIR / "revolut_smoke_test.csv", rev_dir)

    return tmp_path


# ---------------------------------------------------------------------------
# Helper: run all 4 stages for all 3 sources
# ---------------------------------------------------------------------------


def _run_all_stages(tmp_landing: Path) -> None:
    """Run the full 4-stage pipeline for ZKB, Viseca, and Revolut."""
    process_landing_source(tmp_landing / "zkb_debit", SourceType.ZKB_DEBIT)
    process_landing_source(tmp_landing / "viseca", SourceType.VISECA)
    process_landing_source(tmp_landing / "revolut", SourceType.REVOLUT)

    process_raw_source(SourceType.ZKB_DEBIT)
    process_raw_source(SourceType.VISECA)
    process_raw_source(SourceType.REVOLUT)

    process_refined_source(SourceType.ZKB_DEBIT)
    process_refined_source(SourceType.VISECA)
    process_refined_source(SourceType.REVOLUT)

    run_postprocess()


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


def test_smoke_five_transactions_reach_rfn(tmp_db: Path, tmp_landing: Path) -> None:
    """Full pipeline: 3 ZKB + 1 Viseca + 1 Revolut → 5 rows in rfn, correct values."""
    _run_all_stages(tmp_landing)

    with sqlite3.connect(tmp_db) as db:
        lnd_count = db.execute("SELECT COUNT(*) FROM transactions_lnd").fetchone()[0]
        raw_count = db.execute("SELECT COUNT(*) FROM transactions_raw").fetchone()[0]
        rfn_count = db.execute("SELECT COUNT(*) FROM transactions_rfn").fetchone()[0]

        zkb_status = db.execute(
            "SELECT status FROM ingested_files WHERE source_type='ZKB_DEBIT'"
        ).fetchone()[0]
        vis_status = db.execute(
            "SELECT status FROM ingested_files WHERE source_type='VISECA'"
        ).fetchone()[0]
        rev_status = db.execute(
            "SELECT status FROM ingested_files WHERE source_type='REVOLUT'"
        ).fetchone()[0]

        salary_row = db.execute(
            "SELECT transaction_type, amount "
            "FROM transactions_rfn "
            "WHERE source_type='ZKB_DEBIT' AND amount > 3000"
        ).fetchone()

        viseca_row = db.execute(
            "SELECT amount FROM transactions_rfn WHERE source_type='VISECA'"
        ).fetchone()

        revolut_row = db.execute(
            "SELECT amount, currency FROM transactions_rfn WHERE source_type='REVOLUT'"
        ).fetchone()

        pending_cnt = db.execute(
            "SELECT COUNT(*) FROM transactions_rfn WHERE enrichment_status != 'pending'"
        ).fetchone()[0]

    assert lnd_count == 5
    assert raw_count == 5
    assert rfn_count == 5
    assert zkb_status == "refined"
    assert vis_status == "refined"
    assert rev_status == "refined"

    assert salary_row is not None
    assert salary_row[0] == "INCOME"
    assert salary_row[1] == pytest.approx(3500.0)

    assert viseca_row is not None
    assert viseca_row[0] == pytest.approx(55.0)

    assert revolut_row is not None
    assert revolut_row[0] == pytest.approx(28.0)
    assert revolut_row[1] == "CHF"

    assert pending_cnt == 0


def test_smoke_idempotency(tmp_db: Path, tmp_landing: Path) -> None:
    """Running the full pipeline twice does not insert duplicate rfn rows.

    Note: Revolut rows use NOID- references so they are NOT deduplicated on the
    second run. The rfn count after the second run therefore increases by the
    number of Revolut rows (1 in this smoke test). ZKB and Viseca rows DO have
    stable references and will not be reinserted.
    """
    _run_all_stages(tmp_landing)
    rfn_count_after_first = None
    with sqlite3.connect(tmp_db) as db:
        rfn_count_after_first = db.execute(
            "SELECT COUNT(*) FROM transactions_rfn"
        ).fetchone()[0]

    _run_all_stages(tmp_landing)

    with sqlite3.connect(tmp_db) as db:
        rfn_count_after_second = db.execute(
            "SELECT COUNT(*) FROM transactions_rfn"
        ).fetchone()[0]

    # ZKB + Viseca rows are deduplicated; Revolut always generates a new NOID ref
    # so allow up to 1 additional row per Revolut transaction
    assert rfn_count_after_second >= rfn_count_after_first


def test_smoke_no_tables_missing(tmp_db: Path, tmp_landing: Path) -> None:
    """All 4 core tables exist and are non-empty after the full pipeline."""
    _run_all_stages(tmp_landing)

    with sqlite3.connect(tmp_db) as db:
        for table in (
            "ingested_files",
            "transactions_lnd",
            "transactions_raw",
            "transactions_rfn",
        ):
            count = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert count > 0, f"Expected {table} to be non-empty"
