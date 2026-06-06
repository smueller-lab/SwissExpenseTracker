from __future__ import annotations

import sqlite3

import pandas as pd

from swiss_exp_tracker.db.sql import positions
from swiss_exp_tracker.pipeline_ingestion.config import POSITIONS_DB_PATH

_EMPTY_COLUMNS: list[str] = [
    "date",
    "symbol",
    "name",
    "value_chf",
    "pnl_chf",
    "pnl_pct",
    "invested_chf",
]


class PositionsLoader:
    def __init__(self) -> None:
        """Load positions from SQLite; falls back to an empty DataFrame when the DB or its tables are missing."""
        try:
            with sqlite3.connect(str(POSITIONS_DB_PATH)) as con:
                self.pdf = pd.read_sql(positions.get_positions_use.sql, con)

            self.pdf["date"] = pd.to_datetime(self.pdf["date"])
            self.pdf["pnl_chf"] = self.pdf["pnl_chf"].fillna(0.0)
            self.pdf["invested_chf"] = self.pdf["value_chf"] - self.pdf["pnl_chf"]
        except (sqlite3.OperationalError, pd.errors.DatabaseError):
            self.pdf = pd.DataFrame(columns=_EMPTY_COLUMNS)
            self.pdf["date"] = pd.to_datetime(self.pdf["date"])

        if self.pdf.empty:
            self.latest_date: pd.Timestamp | None = None
            self.pdf_latest = self.pdf.copy()
            self.all_symbols: list[str] = []
            self.symbol_to_name: dict[str, str] = {}
            self.total_invested = 0.0
            self.total_value = 0.0
            self.total_pnl_chf = 0.0
            self.total_pnl_pct = 0.0
            return

        self.latest_date = self.pdf["date"].max()
        self.pdf_latest = self.pdf[self.pdf["date"] == self.latest_date].copy()
        self.all_symbols = sorted(self.pdf["symbol"].unique().tolist())

        # symbol → full name (from latest snapshot; falls back to symbol if missing)
        self.symbol_to_name = {
            row["symbol"]: row["name"] if row["name"] else row["symbol"]
            for _, row in self.pdf_latest[["symbol", "name"]].iterrows()
        }

        total_invested = float(self.pdf_latest["invested_chf"].sum())
        self.total_invested = total_invested
        self.total_value = float(self.pdf_latest["value_chf"].sum())
        self.total_pnl_chf = float(self.pdf_latest["pnl_chf"].sum())
        self.total_pnl_pct = (
            self.total_pnl_chf / total_invested * 100 if total_invested > 0 else 0.0
        )
