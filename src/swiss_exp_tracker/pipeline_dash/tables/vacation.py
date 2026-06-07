from __future__ import annotations

import sqlite3

import pandas as pd


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    """Build dash_vacation and dash_vacation_transactions tables from unified transactions."""
    pdf = df[
        (df["transaction_type"] == "EXPENSE") & (df["category_main"] == "Travel")
    ].copy()
    pdf = pdf[pdf["date"].notna()].copy()
    pdf["Year"] = pdf["date"].dt.year.astype(int)

    result = (
        pdf.groupby(["Year", "category_second"], as_index=False)[["amount"]]
        .sum()
        .rename(columns={"amount": "Total"})
    )

    result.to_sql("dash_vacation", con, if_exists="replace", index=False)

    transactions = pdf[["category_second", "amount"]].copy()
    transactions.to_sql(
        "dash_vacation_transactions", con, if_exists="replace", index=False
    )
