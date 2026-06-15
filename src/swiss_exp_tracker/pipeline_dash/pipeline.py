from __future__ import annotations

import json
import logging
import sqlite3

from pathlib import Path

import pandas as pd

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_dash.config import GLOBAL_EXCLUDE
from swiss_exp_tracker.pipeline_dash.tables import balance
from swiss_exp_tracker.pipeline_dash.tables import balance_sheet
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
    rfn_cols = {row[1] for row in transactions.get_rfn_column_names(con)}
    if "balance_chf" not in rfn_cols:
        con.execute(transactions.alter_rfn_add_balance_chf.sql)
        rows = transactions.get_rfn_rows_for_balance_backfill(con)
        for row_id, raw_json_str in rows:
            data = json.loads(raw_json_str)
            balance_val = data.get("Balance CHF")
            if balance_val is not None:
                transactions.set_rfn_balance_chf(
                    con, balance_chf=float(balance_val), rfn_id=row_id
                )

    use_cols = {row[1] for row in transactions.get_use_column_names(con)}
    if "balance_chf" not in use_cols:
        con.execute(transactions.alter_transactions_use_add_balance_chf.sql)

    transactions.backfill_transactions_use_balance_chf(con)
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

        df = pd.read_sql(transactions.get_transactions_use.sql, con)
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
            ("dash_sport + dash_sport_activities", sport),
            ("dash_car", car),
            ("dash_retail + dash_retail_donut + dash_retail_top", retail),
            (
                "dash_balance_sheet + dash_balance_sheet_categories",
                balance_sheet,
            ),
        ]

        for table_name, module in builders:
            logger.debug("Building %s", table_name)
            module.build(df, con)

    logger.info("Dashboard tables built successfully (%d builders)", len(builders))
