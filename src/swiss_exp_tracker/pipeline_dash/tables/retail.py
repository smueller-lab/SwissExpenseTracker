from __future__ import annotations

import sqlite3

import pandas as pd


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    pdf = df[
        (df["transaction_type"] == "EXPENSE") & (df["category_main"] == "Retail")
    ].copy()
    pdf = pdf[pdf["date"].notna()].copy()

    pdf["Year"] = pdf["date"].dt.year.astype(int)
    pdf["MonthYear"] = pdf["date"].dt.to_period("M")

    yearly = (
        pdf.groupby(["Year", "category_second"], as_index=False)[["amount"]]
        .sum()
        .rename(columns={"amount": "Total", "category_second": "category_retail"})
    )
    monthly = (
        pdf.groupby(["MonthYear", "category_second"], as_index=False)[["amount"]]
        .sum()
        .rename(columns={"amount": "Total", "category_second": "category_retail"})
    )

    result = pd.concat(
        [
            yearly.assign(Freq="Yearly", Period=yearly["Year"].astype(str)),
            monthly.assign(Freq="Monthly", Period=monthly["MonthYear"].astype(str)),
        ]
    ).drop(columns=["Year", "MonthYear"])

    result.to_sql("dash_retail", con, if_exists="replace", index=False)

    # Donut: total per subcategory per year (+ All)
    donut_year = (
        pdf.groupby(["Year", "category_second"], as_index=False)[["amount"]]
        .sum()
        .rename(columns={"amount": "amount_CHF", "category_second": "category_retail"})
    )
    donut_all = (
        pdf.groupby("category_second", as_index=False)[["amount"]]
        .sum()
        .rename(columns={"amount": "amount_CHF", "category_second": "category_retail"})
        .assign(Year="All")
    )
    donut = pd.concat(
        [donut_year.assign(Year=donut_year["Year"].astype(str)), donut_all]
    )
    donut.to_sql("dash_retail_donut", con, if_exists="replace", index=False)

    # Top-15 most expensive single retail purchases (enough rows to fill the
    # card next to the donut chart, capped by make_table_card's row limit)
    top = (
        pdf.sort_values("amount", ascending=False)
        .head(15)[["date", "merchant", "category_second", "amount"]]
        .rename(
            columns={
                "date": "Date",
                "merchant": "Merchant",
                "category_second": "Subcategory",
                "amount": "amount_CHF",
            }
        )
    )
    top["Date"] = top["Date"].dt.strftime("%d-%m-%Y")
    top.to_sql("dash_retail_top", con, if_exists="replace", index=False)
