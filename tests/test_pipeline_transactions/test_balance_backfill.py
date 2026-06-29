from __future__ import annotations

import json
import sqlite3

from pathlib import Path

import pytest

from swiss_exp_tracker.pipeline_ingestion.db import backfill_balance_chf_rfn


def _insert_pair(
    con: sqlite3.Connection,
    source_type: str,
    raw_json: str,
    balance: float | None = None,
) -> int:
    """Insert a raw row and a refined row referencing it; return the refined id."""
    con.execute(
        "INSERT INTO transactions_raw "
        "(landing_id, source_type, raw_json, source_file, created_at) "
        "VALUES (1, ?, ?, 'f.csv', '2026-01-01')",
        (source_type, raw_json),
    )
    raw_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
    con.execute(
        "INSERT INTO transactions_rfn "
        "(raw_id, source_type, date, amount, transaction_type, currency, "
        "reference, enrichment_status, created_at, balance_chf) "
        "VALUES (?, ?, '2026-01-01', 10.0, 'EXPENSE', 'CHF', 'R1', 'pending', "
        "'2026-01-01', ?)",
        (raw_id, source_type, balance),
    )
    return int(con.execute("SELECT last_insert_rowid()").fetchone()[0])


def _balance(con: sqlite3.Connection, rfn_id: int) -> float | None:
    """Return the balance_chf stored on a refined row."""
    return con.execute(
        "SELECT balance_chf FROM transactions_rfn WHERE id=?", (rfn_id,)
    ).fetchone()[0]


def test_backfill_ubs_debit_from_solde(tmp_db: Path) -> None:
    """A UBS_DEBIT row with NULL balance is filled from the raw 'Solde' value."""
    with sqlite3.connect(tmp_db) as con:
        rfn_id = _insert_pair(con, "UBS_DEBIT", json.dumps({"Solde": 22.87}))
        updated = backfill_balance_chf_rfn(con)
        assert updated == 1
        assert _balance(con, rfn_id) == pytest.approx(22.87)


def test_backfill_zkb_debit_from_balance_chf(tmp_db: Path) -> None:
    """A ZKB_DEBIT row is filled from its 'Balance CHF' raw key."""
    with sqlite3.connect(tmp_db) as con:
        rfn_id = _insert_pair(con, "ZKB_DEBIT", json.dumps({"Balance CHF": 99.5}))
        backfill_balance_chf_rfn(con)
        assert _balance(con, rfn_id) == pytest.approx(99.5)


def test_backfill_skips_already_populated_rows(tmp_db: Path) -> None:
    """Rows that already have a balance are not selected or overwritten."""
    with sqlite3.connect(tmp_db) as con:
        rfn_id = _insert_pair(
            con, "UBS_DEBIT", json.dumps({"Solde": 1.0}), balance=500.0
        )
        updated = backfill_balance_chf_rfn(con)
        assert updated == 0
        assert _balance(con, rfn_id) == pytest.approx(500.0)


def test_backfill_ignores_sources_without_balance(tmp_db: Path) -> None:
    """UBS_CREDIT carries no running balance and is left NULL."""
    with sqlite3.connect(tmp_db) as con:
        rfn_id = _insert_pair(con, "UBS_CREDIT", json.dumps({"Montant": 3.1}))
        updated = backfill_balance_chf_rfn(con)
        assert updated == 0
        assert _balance(con, rfn_id) is None


def test_backfill_skips_row_missing_key(tmp_db: Path) -> None:
    """A debit row whose raw JSON lacks the balance key stays NULL, no error."""
    with sqlite3.connect(tmp_db) as con:
        rfn_id = _insert_pair(con, "UBS_DEBIT", json.dumps({"Debit": -5.0}))
        updated = backfill_balance_chf_rfn(con)
        assert updated == 0
        assert _balance(con, rfn_id) is None
