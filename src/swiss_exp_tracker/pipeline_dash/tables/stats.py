from __future__ import annotations

import sqlite3

import pandas as pd


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    df = df.copy()
    df["Month"] = df["date"].dt.to_period("M")
    df["Year"] = df["date"].dt.year

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

    zkb = df[(df["source_type"] == "ZKB_DEBIT") & df["balance_chf"].notna()]
    balance_current = (
        float(zkb.sort_values("date")["balance_chf"].iloc[-1]) if len(zkb) > 0 else None
    )

    stats = pd.DataFrame(
        [
            {
                "Balance_current": balance_current,
                "Balance_net_3months": float(by_month["net"].tail(3).mean()),
                "Balance_net_12months": float(by_month["net"].tail(12).mean()),
                "Balance_net_currentYear": float(ytd_net),
            }
        ]
    )

    stats.to_sql("dash_stats", con, if_exists="replace", index=False)
