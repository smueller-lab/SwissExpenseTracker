from __future__ import annotations

import sqlite3

import pandas as pd

from swiss_exp_tracker.pipeline_dash.config import TOP_EXPENSES_EXCLUDE_MAIN


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    pdf = df[df["transaction_type"] == "EXPENSE"].copy()
    pdf = pdf[~pdf["category_main"].isin(TOP_EXPENSES_EXCLUDE_MAIN)].copy()

    cutoff = pdf["date"].max() - pd.DateOffset(years=1)
    pdf = pdf[pdf["date"] >= cutoff].copy()
    pdf = pdf.sort_values("amount", ascending=False).head(20)

    result = pdf[["date", "amount", "merchant", "category_main"]].copy()
    result["date"] = result["date"].dt.strftime("%d-%m-%Y")

    result.to_sql("dash_top_expenses", con, if_exists="replace", index=False)
