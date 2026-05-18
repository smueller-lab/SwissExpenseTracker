from __future__ import annotations

import sqlite3

import pandas as pd

from swiss_exp_tracker.pipeline_dash.config import GROCERY_MERCHANT_NORMALIZE
from swiss_exp_tracker.pipeline_dash.config import GROCERY_MERCHANTS_TRACKED


def _normalize(merchant: str) -> str:
    m = merchant.lower()
    for pattern, canonical in GROCERY_MERCHANT_NORMALIZE:
        if pattern in m:
            return canonical
    return merchant


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    mask = (df["category_main"] == "Groceries") & (df["transaction_type"] == "EXPENSE")
    pdf = df[mask].copy()

    pdf["merchant"] = pdf["merchant"].apply(_normalize)
    pdf = pdf[pdf["merchant"].isin(GROCERY_MERCHANTS_TRACKED)].copy()

    pdf["MonthYear"] = pdf["date"].dt.to_period("M").dt.to_timestamp()
    pdf["Year"] = pdf["date"].dt.year

    monthly = (
        pdf.groupby(["MonthYear", "merchant"], as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "total_CHF"})
    )
    monthly["totalPeriod_CHF"] = monthly.groupby("MonthYear")["total_CHF"].transform(
        "sum"
    )
    monthly["pct"] = monthly["total_CHF"] / monthly["totalPeriod_CHF"] * 100

    yearly = (
        pdf.groupby(["Year", "merchant"], as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "total_CHF"})
    )
    yearly["totalPeriod_CHF"] = yearly.groupby("Year")["total_CHF"].transform("sum")
    yearly["pct"] = yearly["total_CHF"] / yearly["totalPeriod_CHF"] * 100

    result = pd.concat(
        [
            monthly.assign(Freq="Monthly", Period=monthly["MonthYear"].astype(str)),
            yearly.assign(Freq="Yearly", Period=yearly["Year"].astype(str)),
        ]
    ).drop(columns=["MonthYear", "Year"])

    result.to_sql("dash_groceries", con, if_exists="replace", index=False)
