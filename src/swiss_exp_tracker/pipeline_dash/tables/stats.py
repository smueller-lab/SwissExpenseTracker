from __future__ import annotations

import sqlite3

import pandas as pd

from swiss_exp_tracker.pipeline_dash.config import BALANCE_SOURCE_TYPES
from swiss_exp_tracker.pipeline_dash.config import NET_BALANCE_EXPENSE_EXCLUDE_MAIN


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    df = df.copy()
    df = df[
        ~(
            (df["transaction_type"] == "EXPENSE")
            & (df["category_main"].isin(NET_BALANCE_EXPENSE_EXCLUDE_MAIN))
        )
    ]
    df = df[df["date"].notna()]
    df["Month"] = df["date"].dt.to_period("M")
    df["Year"] = df["date"].dt.year.astype(int)

    by_month = (
        df.groupby(["Month", "transaction_type"])["amount"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
    )
    by_month.columns.name = None
    by_month = by_month.rename(columns={"EXPENSE": "expense", "INCOME": "income"})
    for col in ("expense", "income"):
        if col not in by_month.columns:
            by_month[col] = 0.0
    by_month["net"] = by_month["income"] - by_month["expense"]
    by_month = by_month.sort_values("Month")

    current_year = int(df["Year"].max())
    ytd = (
        df[df["Year"] == current_year]
        .groupby("transaction_type")["amount"]
        .sum()
        .to_dict()
    )
    ytd_net = ytd.get("INCOME", 0.0) - ytd.get("EXPENSE", 0.0)

    # Current balance = sum of the most recent known balance per debit account,
    # so it works for any balance-carrying source (ZKB, UBS, …), not just ZKB.
    with_balance = df[
        df["source_type"].isin(BALANCE_SOURCE_TYPES) & df["balance_chf"].notna()
    ].sort_values("date")
    balance_current = (
        float(with_balance.groupby("source_type")["balance_chf"].last().sum())
        if len(with_balance) > 0
        else None
    )

    expense_avg_12m = (
        float(by_month["expense"].tail(12).mean()) if len(by_month) > 0 else 0.0
    )

    stats = pd.DataFrame(
        [
            {
                "Balance_current": balance_current,
                "Balance_net_currentYear": float(ytd_net),
                "Expense_avg_12months": expense_avg_12m,
            }
        ]
    )

    stats.to_sql("dash_stats", con, if_exists="replace", index=False)
