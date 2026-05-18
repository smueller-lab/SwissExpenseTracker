from __future__ import annotations

import sqlite3

import pandas as pd


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    df = df.copy()
    df["Month"] = df["date"].dt.to_period("M")

    pdf = df[
        (df["transaction_type"] == "EXPENSE")
        & ~df["category_main"].isin(["Housing", "Government"])
    ].copy()

    date_max = df["date"].max()
    if date_max.is_month_end:
        month_last = date_max.to_period("M")
    else:
        month_last = (date_max - pd.offsets.MonthEnd(1)).to_period("M")

    month_prev = month_last - 1
    month_start_12m = month_last - 11

    by_month = pdf.groupby(["Month", "category_main"], as_index=False)["amount"].sum()

    last = by_month[by_month["Month"] == month_last].rename(
        columns={"amount": "amount_MonthLast"}
    )
    prev = by_month[by_month["Month"] == month_prev].rename(
        columns={"amount": "amount_MonthPrev"}
    )
    avg_12m = (
        by_month[
            (by_month["Month"] >= month_start_12m) & (by_month["Month"] <= month_last)
        ]
        .groupby("category_main", as_index=False)["amount"]
        .mean()
        .rename(columns={"amount": "amount_AVG_12m"})
    )

    result = (
        last[["category_main", "amount_MonthLast"]]
        .merge(
            prev[["category_main", "amount_MonthPrev"]], on="category_main", how="left"
        )
        .merge(avg_12m, on="category_main", how="left")
        .sort_values("amount_MonthLast", ascending=False)
        .reset_index(drop=True)
    )

    result["diff_prev_pct"] = (
        (result["amount_MonthLast"] - result["amount_MonthPrev"])
        / result["amount_MonthPrev"]
        * 100
    )
    result["diff_12m_pct"] = (
        (result["amount_MonthLast"] - result["amount_AVG_12m"])
        / result["amount_AVG_12m"]
        * 100
    )
    result["MonthLast"] = str(month_last)

    result.to_sql("dash_top_category", con, if_exists="replace", index=False)
