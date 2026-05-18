from __future__ import annotations

import sqlite3

import pandas as pd

from swiss_exp_tracker.pipeline_dash.config import NET_BALANCE_EXPENSE_EXCLUDE_MAIN


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    df = df.copy()
    df = df[
        ~(
            (df["transaction_type"] == "EXPENSE")
            & (df["category_main"].isin(NET_BALANCE_EXPENSE_EXCLUDE_MAIN))
        )
    ]
    df["Month"] = df["date"].dt.to_period("M")

    pivot = (
        df.groupby(["transaction_type", "Month"])["amount"]
        .sum()
        .reset_index()
        .pivot_table(
            index="Month", columns="transaction_type", values="amount", fill_value=0
        )
        .reset_index()
    )
    pivot.columns.name = None
    pivot = pivot.rename(columns={"EXPENSE": "expense", "INCOME": "income"})

    for col in ("expense", "income"):
        if col not in pivot.columns:
            pivot[col] = 0.0

    pivot["NetBalance"] = pivot["income"] - pivot["expense"]
    pivot = pivot.sort_values("Month", ascending=False)
    pivot["Month"] = pivot["Month"].astype(str)

    pivot.to_sql("dash_net_balance_month", con, if_exists="replace", index=False)
