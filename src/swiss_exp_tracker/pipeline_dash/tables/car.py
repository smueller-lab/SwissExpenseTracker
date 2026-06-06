from __future__ import annotations

import sqlite3

import pandas as pd

CAR_EXCLUDE_SECOND: list[str] = ["Purchase", "Car Rental"]


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    pdf = df[
        (df["transaction_type"] == "EXPENSE") & (df["category_main"] == "Car")
    ].copy()
    pdf = pdf[~pdf["category_second"].isin(CAR_EXCLUDE_SECOND)].copy()

    pdf = pdf[pdf["date"].notna()].copy()
    pdf["Year"] = pdf["date"].dt.year.astype(int)
    pdf["MonthYear"] = pdf["date"].dt.to_period("M")

    yearly = (
        pdf.groupby(["Year", "category_second"], as_index=False)[["amount"]]
        .sum()
        .rename(columns={"amount": "Total", "category_second": "category_car"})
    )
    monthly = (
        pdf.groupby(["MonthYear", "category_second"], as_index=False)[["amount"]]
        .sum()
        .rename(columns={"amount": "Total", "category_second": "category_car"})
    )

    result = pd.concat(
        [
            yearly.assign(Freq="Yearly", Period=yearly["Year"].astype(str)),
            monthly.assign(Freq="Monthly", Period=monthly["MonthYear"].astype(str)),
        ]
    ).drop(columns=["Year", "MonthYear"])

    result.to_sql("dash_car", con, if_exists="replace", index=False)
