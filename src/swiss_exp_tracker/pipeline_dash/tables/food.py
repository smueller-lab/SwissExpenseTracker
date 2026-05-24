from __future__ import annotations

import sqlite3

import pandas as pd


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    groceries = df[df["category_main"] == "Groceries"].copy()
    groceries["category_second"] = "Groceries"
    restaurant = df[df["category_main"] == "Restaurant"].copy()

    pdf = pd.concat([groceries, restaurant], ignore_index=True)
    pdf = pdf[pdf["transaction_type"] == "EXPENSE"].copy()

    pdf["MonthYear"] = pdf["date"].dt.to_period("M").dt.to_timestamp()
    pdf["Year"] = pdf["date"].dt.year

    monthly = (
        pdf.groupby(["MonthYear", "category_second"], as_index=False)[["amount"]]
        .sum()
        .rename(columns={"amount": "total_CHF"})
    )
    monthly["totalPeriod_CHF"] = monthly.groupby("MonthYear")["total_CHF"].transform(
        "sum"
    )
    monthly["pct"] = monthly["total_CHF"] / monthly["totalPeriod_CHF"] * 100

    yearly = (
        pdf.groupby(["Year", "category_second"], as_index=False)[["amount"]]
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

    result.to_sql("dash_food", con, if_exists="replace", index=False)
