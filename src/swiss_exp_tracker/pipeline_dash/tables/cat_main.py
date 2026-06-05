from __future__ import annotations

import sqlite3

import pandas as pd


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    pdf = df[(df["transaction_type"] == "EXPENSE") & df["date"].notna()].copy()
    pdf["Year"] = pdf["date"].dt.year.astype(int).astype(str)

    by_year = pdf.groupby(["category_main", "Year"], as_index=False)["amount"].sum()
    all_years = pdf.groupby("category_main", as_index=False)["amount"].sum()
    all_years["Year"] = "All"

    result = pd.concat([by_year, all_years], ignore_index=True)
    result["perc"] = result.groupby("Year")["amount"].transform(
        lambda x: x / x.sum() * 100  # pyright: ignore[reportUnknownLambdaType]
    )

    big = result[result["perc"] >= 1].copy()
    small = result[result["perc"] < 1].copy()

    other = small.groupby("Year").agg({"amount": "sum", "perc": "sum"}).reset_index()
    other["category_main"] = "Other"

    result = pd.concat([big, other], ignore_index=True)
    result = result.sort_values(["Year", "category_main"]).reset_index(drop=True)
    result["Year"] = result["Year"].astype(str)

    result.to_sql("dash_cat_main", con, if_exists="replace", index=False)
