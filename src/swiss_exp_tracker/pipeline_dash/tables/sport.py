from __future__ import annotations

import sqlite3

import pandas as pd

from swiss_exp_tracker.pipeline_dash.config import SPORT_EXCLUDE_SECOND

_GOLF_MIN = 70
_GOLF_MAX = 160


def build_activities(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    """Count qualifying activity transactions per year per sport (Golf/Tennis/Padel).

    Writes dash_sport_activities with columns: year (int), sport (str), activity_count (int).
    """
    pdf = df[
        (df["transaction_type"] == "EXPENSE") & (df["category_main"] == "Sport")
    ].copy()
    pdf = pdf[pdf["date"].notna()].copy()

    golf = pdf[
        (pdf["category_second"] == "Golf")
        & (pdf["amount"] >= _GOLF_MIN)
        & (pdf["amount"] <= _GOLF_MAX)
    ].copy()
    golf["sport"] = "Golf"

    tennis = pdf[pdf["category_second"] == "Tennis"].copy()
    tennis["sport"] = "Tennis"

    padel = pdf[pdf["category_second"] == "Padel"].copy()
    padel["sport"] = "Padel"

    combined = pd.concat([golf, tennis, padel])
    combined["year"] = combined["date"].dt.year.astype(int)

    result = (
        combined.groupby(["year", "sport"], as_index=False)
        .size()
        .rename(columns={"size": "activity_count"})
    )

    result.to_sql("dash_sport_activities", con, if_exists="replace", index=False)


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    """Build dash_sport (yearly/monthly totals by category) and dash_sport_activities."""
    pdf = df[
        (df["transaction_type"] == "EXPENSE") & (df["category_main"] == "Sport")
    ].copy()
    pdf = pdf[~pdf["category_second"].isin(SPORT_EXCLUDE_SECOND)].copy()
    pdf = pdf[pdf["date"].notna()].copy()

    pdf["Year"] = pdf["date"].dt.year.astype(int)
    pdf["MonthYear"] = pdf["date"].dt.to_period("M")

    yearly = (
        pdf.groupby(["Year", "category_second"], as_index=False)[["amount"]]
        .sum()
        .rename(columns={"amount": "Total", "category_second": "category_sport"})
    )
    monthly = (
        pdf.groupby(["MonthYear", "category_second"], as_index=False)[["amount"]]
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
    build_activities(df, con)
