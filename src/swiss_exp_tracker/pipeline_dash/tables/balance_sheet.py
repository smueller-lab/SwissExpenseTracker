from __future__ import annotations

import sqlite3

import pandas as pd

from swiss_exp_tracker.pipeline_dash.config import BALANCE_SHEET_MAJOR_CATEGORIES
from swiss_exp_tracker.pipeline_dash.config import NET_BALANCE_EXPENSE_EXCLUDE_MAIN


def _aggregate_yearly(df: pd.DataFrame) -> pd.DataFrame:
    """Return yearly income, expense, and invested totals; rows indexed by year (int)."""
    df = df.copy()
    df["year"] = df["date"].dt.year

    income = (
        df[df["transaction_type"] == "INCOME"]
        .groupby("year")["amount"]
        .sum()
        .rename("income")
    )
    expense = (
        df[
            (df["transaction_type"] == "EXPENSE")
            & (~df["category_main"].isin(NET_BALANCE_EXPENSE_EXCLUDE_MAIN))
        ]
        .groupby("year")["amount"]
        .sum()
        .rename("expense")
    )
    invested = (
        df[(df["transaction_type"] == "EXPENSE") & (df["category_main"] == "Investing")]
        .groupby("year")["amount"]
        .sum()
        .rename("invested")
    )

    result = pd.concat([income, expense, invested], axis=1).fillna(0.0)
    result.index.name = "year"
    return result.reset_index()


def _compute_balance_sheet(agg: pd.DataFrame) -> pd.DataFrame:
    """Derive saved, rates, cumulative_saved, and yoy_saved_pct from yearly aggregates.

    cumulative_saved and yoy_saved_pct require year-ascending ordering internally;
    the final output is sorted year descending (newest first).
    """
    agg = agg.copy()
    agg["saved"] = agg["income"] - agg["expense"] - agg["invested"]
    agg["total_plus"] = agg["saved"] + agg["invested"]
    agg["in_plus"] = (agg["saved"] >= 0).astype(int)

    # Guard division by zero: replace non-positive income with NaN for rate division
    safe_income = agg["income"].where(agg["income"] > 0, other=float("nan"))
    agg["savings_rate_pct"] = (agg["total_plus"] / safe_income * 100).fillna(0.0)
    agg["invested_rate_pct"] = (agg["invested"] / safe_income * 100).fillna(0.0)
    agg["expense_rate_pct"] = (agg["expense"] / safe_income * 100).fillna(0.0)

    agg = agg.sort_values("year", ascending=True).reset_index(drop=True)
    agg["cumulative_saved"] = agg["total_plus"].cumsum()

    prev_total_plus = agg["total_plus"].shift(1)
    # NaN comparisons evaluate to False, so the first row (NaN prev) gets 0.0 here
    yoy = (agg["total_plus"] - prev_total_plus) / prev_total_plus.abs() * 100
    agg["yoy_saved_pct"] = yoy.where(prev_total_plus != 0, other=0.0).fillna(0.0)

    agg = agg.sort_values("year", ascending=False).reset_index(drop=True)
    return agg[
        [
            "year",
            "income",
            "expense",
            "invested",
            "saved",
            "total_plus",
            "savings_rate_pct",
            "invested_rate_pct",
            "expense_rate_pct",
            "cumulative_saved",
            "yoy_saved_pct",
            "in_plus",
        ]
    ]


def _compute_category_spend(df: pd.DataFrame) -> pd.DataFrame:
    """Return long-format yearly expense totals for each label in BALANCE_SHEET_MAJOR_CATEGORIES."""
    df = df.copy()
    df["year"] = df["date"].dt.year
    expense_df = df[df["transaction_type"] == "EXPENSE"]

    rows: list[pd.DataFrame] = []
    for label, cat_main, cat_second in BALANCE_SHEET_MAJOR_CATEGORIES:
        mask = expense_df["category_main"] == cat_main
        if cat_second is not None:
            mask = mask & (expense_df["category_second"] == cat_second)
        yearly = expense_df[mask].groupby("year")["amount"].sum().reset_index()
        yearly["category"] = label
        rows.append(yearly)

    if not rows:
        return pd.DataFrame(columns=["year", "category", "amount"])

    return pd.concat(rows, ignore_index=True)[["year", "category", "amount"]]


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    """Write dash_balance_sheet and dash_balance_sheet_categories from transactions_use."""
    agg = _aggregate_yearly(df)
    balance_sheet = _compute_balance_sheet(agg)
    balance_sheet["year"] = balance_sheet["year"].astype(int)
    balance_sheet.to_sql("dash_balance_sheet", con, if_exists="replace", index=False)

    categories = _compute_category_spend(df)
    categories["year"] = categories["year"].astype(int)
    categories.to_sql(
        "dash_balance_sheet_categories", con, if_exists="replace", index=False
    )
