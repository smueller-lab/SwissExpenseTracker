from __future__ import annotations

import sqlite3

import pandas as pd

from swiss_exp_tracker.pipeline_dash.config import SPORT_EXCLUDE_SECOND


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    pdf = df[
        (df["transaction_type"] == "EXPENSE") & (df["category_main"] == "Sport")
    ].copy()
    pdf = pdf[~pdf["category_second"].isin(SPORT_EXCLUDE_SECOND)].copy()

    pdf["Year"] = pdf["date"].dt.year
    pdf["MonthYear"] = pdf["date"].dt.to_period("M")

    yearly = (
        pdf.groupby(["Year", "category_second"], as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "Total", "category_second": "category_sport"})
    )
    monthly = (
        pdf.groupby(["MonthYear", "category_second"], as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "Total", "category_second": "category_sport"})
    )

    result = pd.concat(
        [
            yearly.assign(Freq="Yearly", Period=yearly["Year"].astype(str)),
            monthly.assign(Freq="Monthly", Period=monthly["MonthYear"].astype(str)),
        ]
    ).drop(columns=["Year", "MonthYear"])

    result.to_sql("dash_sport", con, if_exists="replace", index=False)
