from __future__ import annotations

import json
import logging
import sqlite3

from pathlib import Path

import pandas as pd

from swiss_exp_tracker.pipeline_dash.config import GLOBAL_EXCLUDE
from swiss_exp_tracker.pipeline_dash.tables import balance
from swiss_exp_tracker.pipeline_dash.tables import car
from swiss_exp_tracker.pipeline_dash.tables import cat_main
from swiss_exp_tracker.pipeline_dash.tables import food
from swiss_exp_tracker.pipeline_dash.tables import groceries
from swiss_exp_tracker.pipeline_dash.tables import groceries_detail
from swiss_exp_tracker.pipeline_dash.tables import net_balance_month
from swiss_exp_tracker.pipeline_dash.tables import retail
from swiss_exp_tracker.pipeline_dash.tables import sport
from swiss_exp_tracker.pipeline_dash.tables import stats
from swiss_exp_tracker.pipeline_dash.tables import top_category
from swiss_exp_tracker.pipeline_dash.tables import top_expenses
from swiss_exp_tracker.pipeline_dash.tables import transport
from swiss_exp_tracker.pipeline_dash.tables import vacation


logger = logging.getLogger(__name__)


def _ensure_balance_chf(con: sqlite3.Connection) -> None:
    """Migrate balance_chf into transactions_rfn and transactions_use if absent.

    Runs as part of run_dashboard_pipeline so the dashboard can build correctly
    even when called on a DB that pre-dates the balance_chf column.
    """
    rfn_cols = {
        row[1] for row in con.execute("PRAGMA table_info(transactions_rfn)").fetchall()
    }
    if "balance_chf" not in rfn_cols:
        con.execute("ALTER TABLE transactions_rfn ADD COLUMN balance_chf REAL")
        rows = con.execute(
            """
            SELECT rfn.id, raw.raw_json
            FROM transactions_rfn rfn
            JOIN transactions_raw raw ON raw.id = rfn.raw_id
            WHERE rfn.source_type = 'ZKB_DEBIT'
            """
        ).fetchall()
        for row_id, raw_json_str in rows:
            data = json.loads(raw_json_str)
            balance_val = data.get("Balance CHF")
            if balance_val is not None:
                con.execute(
                    "UPDATE transactions_rfn SET balance_chf = ? WHERE id = ?",
                    (float(balance_val), row_id),
                )

    use_cols = {
        row[1] for row in con.execute("PRAGMA table_info(transactions_use)").fetchall()
    }
    if "balance_chf" not in use_cols:
        con.execute("ALTER TABLE transactions_use ADD COLUMN balance_chf REAL")

    con.execute(
        """
        UPDATE transactions_use
        SET balance_chf = (
            SELECT t.balance_chf FROM transactions_rfn t
            WHERE t.reference = transactions_use.reference
        )
        WHERE balance_chf IS NULL
        """
    )
    con.commit()


def run_dashboard_pipeline(db_path: Path | None = None) -> None:
    """Read transactions_use and write all dash_* tables into the same database.

    Args:
        db_path: Path to the SQLite database. Defaults to the project database.
    """
    if db_path is None:
        from swiss_exp_tracker.pipeline_ingestion.config import INGESTION_DB_PATH

        db_path = INGESTION_DB_PATH

    with sqlite3.connect(db_path) as con:
        _ensure_balance_chf(con)

        df = pd.read_sql("SELECT * FROM transactions_use", con)
        df["date"] = pd.to_datetime(df["date"])

        for cat_main_val, cat_second_val, merchant_substr in GLOBAL_EXCLUDE:
            mask = (df["category_main"] == cat_main_val) & (
                df["category_second"] == cat_second_val
            )
            if merchant_substr is not None:
                mask &= df["merchant"].str.contains(
                    merchant_substr, case=False, na=False
                )
            df = df[~mask]

        builders = [
            ("dash_balance", balance),
            ("dash_groceries", groceries),
            (
                "dash_groceries_cat + dash_groceries_health + dash_groceries_top_articles",
                groceries_detail,
            ),
            ("dash_food", food),
            ("dash_cat_main", cat_main),
            ("dash_stats", stats),
            ("dash_top_category", top_category),
            ("dash_top_expenses", top_expenses),
            ("dash_net_balance_month", net_balance_month),
            ("dash_vacation", vacation),
            ("dash_transport + dash_transport_heatmap", transport),
            ("dash_sport", sport),
            ("dash_car", car),
            ("dash_retail + dash_retail_donut + dash_retail_top", retail),
        ]

        for table_name, module in builders:
            logger.info("Building %s", table_name)
            module.build(df, con)

    logger.info("Dashboard tables built successfully")
