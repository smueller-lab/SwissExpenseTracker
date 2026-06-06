from __future__ import annotations

import csv
import json
import shutil
import sqlite3

from pathlib import Path

import pytest

from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_01_landing import (
    process_landing_source,
)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"

# ---------------------------------------------------------------------------
# CSV writer helpers
# ---------------------------------------------------------------------------

_ZKB_FIELDNAMES = [
    "Date",
    "Booking text",
    "Curr",
    "Amount details",
    "ZKB reference",
    "Reference number",
    "Debit CHF",
    "Credit CHF",
    "Value date",
    "Balance CHF",
    "Payment purpose",
    "Details",
]

_VISECA_FIELDNAMES = [
    "TransactionId",
    "CardId",
    "Date",
    "ValutaDate",
    "Amount",
    "Currency",
    "OriginalAmount",
    "OriginalCurrency",
    "MerchantName",
    "MerchantPlace",
    "MerchantCountry",
    "StateType",
    "Details",
    "Type",
    "Exchange Rate",
]

_REVOLUT_FIELDNAMES = [
    "Type",
    "Product",
    "Started Date",
    "Completed Date",
    "Description",
    "Amount",
    "Fee",
    "Currency",
    "State",
    "Balance",
]


def _write_zkb_csv(folder: Path, rows: list[dict[str, object]]) -> Path:
    """Write a ZKB CSV file from a list of row dicts into folder."""
    path = folder / "test_zkb.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_ZKB_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _write_viseca_csv(folder: Path, rows: list[dict[str, object]]) -> Path:
    """Write a Viseca CSV file from a list of row dicts into folder."""
    path = folder / "test_viseca.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_VISECA_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _write_revolut_csv(folder: Path, rows: list[dict[str, object]]) -> Path:
    """Write a Revolut CSV file from a list of row dicts into folder."""
    path = folder / "test_revolut.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_REVOLUT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _make_zkb_row_dict(**overrides: object) -> dict[str, object]:
    """Return a valid ZKB row dict; each caller only states what it varies."""
    base: dict[str, object] = {
        "Date": "12.01.2025",
        "Booking text": "Debit TWINT: Test",
        "Curr": "",
        "Amount details": "",
        "ZKB reference": "L99999",
        "Reference number": "",
        "Debit CHF": "10.00",
        "Credit CHF": "",
        "Value date": "12.01.2025",
        "Balance CHF": "1000.00",
        "Payment purpose": "",
        "Details": "",
    }
    base.update(overrides)
    return base


def _make_viseca_row_dict(**overrides: object) -> dict[str, object]:
    """Return a valid Viseca row dict; each caller only states what it varies."""
    base: dict[str, object] = {
        "TransactionId": "T999",
        "CardId": "723",
        "Date": "2023-05-23 06:27:05",
        "ValutaDate": "2023-05-24 00:00:00",
        "Amount": "20.0",
        "Currency": "CHF",
        "OriginalAmount": "20.0",
        "OriginalCurrency": "CHF",
        "MerchantName": "Test Merchant",
        "MerchantPlace": "Location",
        "MerchantCountry": "CHE",
        "StateType": "BOOKED",
        "Details": "Test Merchant",
        "Type": "purchase",
        "Exchange Rate": "1",
    }
    base.update(overrides)
    return base


def _make_revolut_row_dict(**overrides: object) -> dict[str, object]:
    """Return a valid Revolut row dict; each caller only states what it varies."""
    base: dict[str, object] = {
        "Type": "Payment",
        "Product": "Current",
        "Started Date": "2022-11-22 20:31:00",
        "Completed Date": "2022-11-22 20:31:00",
        "Description": "Test Payment",
        "Amount": "-10.0",
        "Fee": "0",
        "Currency": "CHF",
        "State": "COMPLETED",
        "Balance": "100",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


def test_happy_path_zkb(tmp_path: Path, tmp_db: Path) -> None:
    """ZKB CSV with 4 rows → files_found=1, records_inserted=4, status=landing."""
    folder = tmp_path / "zkb_debit"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "zkb_test.csv", folder)

    result = process_landing_source(folder, SourceType.ZKB_DEBIT)

    assert result["files_found"] == 1
    assert result["files_processed"] == 1
    assert result["records_inserted"] == 4

    with sqlite3.connect(tmp_db) as db:
        lnd_count = db.execute("SELECT COUNT(*) FROM transactions_lnd").fetchone()[0]
        file_status = db.execute(
            "SELECT status FROM ingested_files WHERE source_type='ZKB_DEBIT'"
        ).fetchone()[0]

    assert lnd_count == 4
    assert file_status == "landing"


def test_happy_path_viseca(tmp_path: Path, tmp_db: Path) -> None:
    """Viseca CSV → at least 1 record inserted into landing."""
    folder = tmp_path / "viseca"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "viseca_test.csv", folder)

    result = process_landing_source(folder, SourceType.VISECA)

    assert result["records_inserted"] >= 1


def test_happy_path_revolut(tmp_path: Path, tmp_db: Path) -> None:
    """Revolut CSV → at least 1 record inserted into landing."""
    folder = tmp_path / "revolut"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "account_statement_EUR_test.csv", folder)

    result = process_landing_source(folder, SourceType.REVOLUT)

    assert result["records_inserted"] >= 1


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_idempotency(tmp_path: Path, tmp_db: Path) -> None:
    """Second run with same file returns files_found=0, records_inserted=0."""
    folder = tmp_path / "zkb_debit"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "zkb_test.csv", folder)

    process_landing_source(folder, SourceType.ZKB_DEBIT)
    result2 = process_landing_source(folder, SourceType.ZKB_DEBIT)

    assert result2["files_found"] == 0
    assert result2["records_inserted"] == 0

    with sqlite3.connect(tmp_db) as db:
        lnd_count = db.execute("SELECT COUNT(*) FROM transactions_lnd").fetchone()[0]
    assert lnd_count == 4


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_folder(tmp_path: Path, tmp_db: Path) -> None:
    """Empty folder → all counters zero."""
    folder = tmp_path / "empty"
    folder.mkdir()

    result = process_landing_source(folder, SourceType.ZKB_DEBIT)

    assert result == {"files_found": 0, "files_processed": 0, "records_inserted": 0}


def test_lnd_raw_json_is_valid_json(tmp_path: Path, tmp_db: Path) -> None:
    """raw_json stored in landing is valid JSON and contains Date key."""
    folder = tmp_path / "zkb_debit"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "zkb_test.csv", folder)

    process_landing_source(folder, SourceType.ZKB_DEBIT)

    with sqlite3.connect(tmp_db) as db:
        raw_jsons = db.execute("SELECT raw_json FROM transactions_lnd").fetchall()

    for (raw_json,) in raw_jsons:
        parsed = json.loads(raw_json)
        assert isinstance(parsed, dict)
        assert "Date" in parsed


# ---------------------------------------------------------------------------
# File-tracking tests
# ---------------------------------------------------------------------------


def test_modified_file_treated_as_new(tmp_path: Path, tmp_db: Path) -> None:
    """Same filename with different content is treated as a new file."""
    folder = tmp_path / "zkb_debit"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "zkb_test.csv", folder)
    process_landing_source(folder, SourceType.ZKB_DEBIT)

    # Overwrite the same filename with different content (5 rows)
    rows = [_make_zkb_row_dict(**{"ZKB reference": f"L{i:05d}"}) for i in range(5)]
    _write_zkb_csv(folder, rows)

    result2 = process_landing_source(folder, SourceType.ZKB_DEBIT)

    assert result2["files_found"] == 1
    assert result2["files_processed"] == 1

    with sqlite3.connect(tmp_db) as db:
        file_count = db.execute("SELECT COUNT(*) FROM ingested_files").fetchone()[0]
    assert file_count == 2


def test_two_files_both_processed_in_one_run(tmp_path: Path, tmp_db: Path) -> None:
    """Two distinct ZKB files in one folder → both processed, total records = 4 + 3."""
    folder = tmp_path / "zkb_debit"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "zkb_test.csv", folder)
    shutil.copy(TEST_DATA_DIR / "zkb_smoke_test.csv", folder)

    result = process_landing_source(folder, SourceType.ZKB_DEBIT)

    assert result["files_processed"] == 2
    assert result["records_inserted"] == 7


def test_second_file_picked_up_after_first_run(tmp_path: Path, tmp_db: Path) -> None:
    """Second run picks up only the newly added file."""
    folder = tmp_path / "zkb_debit"
    folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "zkb_test.csv", folder)

    process_landing_source(folder, SourceType.ZKB_DEBIT)
    shutil.copy(TEST_DATA_DIR / "zkb_smoke_test.csv", folder)

    result2 = process_landing_source(folder, SourceType.ZKB_DEBIT)

    assert result2["files_found"] == 1
    assert result2["files_processed"] == 1

    with sqlite3.connect(tmp_db) as db:
        lnd_count = db.execute("SELECT COUNT(*) FROM transactions_lnd").fetchone()[0]
    assert lnd_count == 4 + 3


def test_source_type_isolation_in_file_tracking(tmp_path: Path, tmp_db: Path) -> None:
    """ZKB and VISECA files tracked independently; re-running ZKB finds 0 new files."""
    zkb_folder = tmp_path / "zkb_debit"
    zkb_folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "zkb_test.csv", zkb_folder)

    vis_folder = tmp_path / "viseca"
    vis_folder.mkdir()
    shutil.copy(TEST_DATA_DIR / "viseca_test.csv", vis_folder)

    process_landing_source(zkb_folder, SourceType.ZKB_DEBIT)
    process_landing_source(vis_folder, SourceType.VISECA)

    with sqlite3.connect(tmp_db) as db:
        file_count = db.execute("SELECT COUNT(*) FROM ingested_files").fetchone()[0]
        sources = {
            row[0]
            for row in db.execute(
                "SELECT DISTINCT source_type FROM ingested_files"
            ).fetchall()
        }

    assert file_count == 2
    assert "ZKB_DEBIT" in sources
    assert "VISECA" in sources

    result_zkb2 = process_landing_source(zkb_folder, SourceType.ZKB_DEBIT)
    assert result_zkb2["files_found"] == 0


# ---------------------------------------------------------------------------
# Datetime variant landing tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "date_str",
    [
        "12.01.2025",
        "2025-01-12",
        "2025-01-12T14:30:00",
        "2025-01-12 14:30:00",
    ],
)
def test_stage01_zkb_date_variants_land_without_error(
    date_str: str, tmp_path: Path, tmp_db: Path
) -> None:
    """ZKB rows with various Date formats are accepted by the landing stage."""
    folder = tmp_path / f"zkb_{date_str.replace(':', '-').replace(' ', '_')}"
    folder.mkdir()
    _write_zkb_csv(
        folder,
        [_make_zkb_row_dict(**{"Date": date_str, "Value date": "12.01.2025"})],
    )
    result = process_landing_source(folder, SourceType.ZKB_DEBIT)
    assert result["records_inserted"] == 1


@pytest.mark.parametrize(
    "date_str",
    [
        "2023-05-23 06:27:05",
        "2023-05-23T06:27:05",
        "2023-05-23",
    ],
)
def test_stage01_viseca_date_variants_land_without_error(
    date_str: str, tmp_path: Path, tmp_db: Path
) -> None:
    """Viseca rows with various Date formats are accepted by the landing stage."""
    folder = tmp_path / f"viseca_{date_str.replace(':', '-').replace(' ', '_')}"
    folder.mkdir()
    _write_viseca_csv(
        folder,
        [_make_viseca_row_dict(Date=date_str, ValutaDate="2023-05-24 00:00:00")],
    )
    result = process_landing_source(folder, SourceType.VISECA)
    assert result["records_inserted"] == 1


@pytest.mark.parametrize(
    "date_str",
    [
        "2022-11-22 20:31:00",
        "2022-11-22T20:31:00",
        "2022-11-22",
    ],
)
def test_stage01_revolut_date_variants_land_without_error(
    date_str: str, tmp_path: Path, tmp_db: Path
) -> None:
    """Revolut rows with various Started Date formats are accepted by the landing stage."""
    folder = tmp_path / f"rev_{date_str.replace(':', '-').replace(' ', '_')}"
    folder.mkdir()
    _write_revolut_csv(
        folder,
        [
            _make_revolut_row_dict(
                **{"Started Date": date_str, "Completed Date": "2022-11-22 20:31:00"}
            )
        ],
    )
    result = process_landing_source(folder, SourceType.REVOLUT)
    assert result["records_inserted"] == 1
