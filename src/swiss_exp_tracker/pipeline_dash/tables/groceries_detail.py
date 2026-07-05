from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd

HEALTH_WEIGHTS: dict[str, float] = {
    "Fresh Produce": 1.0,
    "Dairy & Eggs": 0.6,
    "Meat & Fish": 0.4,
    "Pasta & Grains": 0.5,
    "Baking": 0.3,
    "Snacks & Sweets": -1.0,
    "Ready Meals": -0.8,
    "Beverages": -0.4,
    "Frozen Foods": -0.3,
    "Canned & Preserved": -0.1,
    "Bakery & Bread": 0.0,
    "Personal & Household": 0.0,
    "Other": 0.0,
}


# Output table → column order, used to emit empty tables when there is no
# item-level Cumulus data so downstream loader reads still find each table.
_EMPTY_TABLE_COLUMNS: dict[str, list[str]] = {
    "dash_groceries_cat": [
        "category_main",
        "total_CHF",
        "totalPeriod_CHF",
        "pct",
        "Freq",
        "Period",
    ],
    "dash_groceries_health": ["Period", "score"],
    "dash_groceries_top_articles": [
        "article",
        "category_main",
        "count",
        "total_chf",
        "avg_chf",
    ],
}


def _health_score(sub: pd.DataFrame) -> float:
    total = sub["price_chf"].sum()
    if total == 0:
        return 50.0
    score = 50.0
    for cat, weight in HEALTH_WEIGHTS.items():
        share = sub.loc[sub["category_main"] == cat, "price_chf"].sum() / total
        score += 50.0 * weight * share
    return float(np.clip(score, 0.0, 100.0))


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    """Return True if a table named `name` exists in the database."""
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _write_empty_grocery_tables(con: sqlite3.Connection) -> None:
    """Replace the three grocery-detail dash tables with empty, correctly-typed tables."""
    for table_name, columns in _EMPTY_TABLE_COLUMNS.items():
        pd.DataFrame(columns=columns).to_sql(
            table_name, con, if_exists="replace", index=False
        )


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    # Cumulus receipt data is optional; with none present, emit empty tables so
    # the dashboard loader still finds each dash_groceries_* table.
    if not _table_exists(con, "groceries_use"):
        _write_empty_grocery_tables(con)
        return

    items = pd.read_sql("SELECT * FROM groceries_use", con)
    items["date"] = pd.to_datetime(items["date"])
    items = items[items["date"].notna()].copy()
    if items.empty:
        _write_empty_grocery_tables(con)
        return

    items["MonthYear"] = items["date"].dt.to_period("M").dt.to_timestamp()
    items["Year"] = items["date"].dt.year.astype(int)

    # dash_groceries_cat — category spend trend (monthly + yearly)
    monthly_cat = (
        items.groupby(["MonthYear", "category_main"], as_index=False)[["price_chf"]]
        .sum()
        .rename(columns={"price_chf": "total_CHF"})
    )
    monthly_cat["totalPeriod_CHF"] = monthly_cat.groupby("MonthYear")[
        "total_CHF"
    ].transform("sum")
    monthly_cat["pct"] = monthly_cat["total_CHF"] / monthly_cat["totalPeriod_CHF"] * 100

    yearly_cat = (
        items.groupby(["Year", "category_main"], as_index=False)[["price_chf"]]
        .sum()
        .rename(columns={"price_chf": "total_CHF"})
    )
    yearly_cat["totalPeriod_CHF"] = yearly_cat.groupby("Year")["total_CHF"].transform(
        "sum"
    )
    yearly_cat["pct"] = yearly_cat["total_CHF"] / yearly_cat["totalPeriod_CHF"] * 100

    dash_cat = pd.concat(
        [
            monthly_cat.assign(
                Freq="Monthly", Period=monthly_cat["MonthYear"].astype(str)
            ),
            yearly_cat.assign(Freq="Yearly", Period=yearly_cat["Year"].astype(str)),
        ]
    ).drop(columns=["MonthYear", "Year"])
    dash_cat.to_sql("dash_groceries_cat", con, if_exists="replace", index=False)

    # dash_groceries_health — monthly health index score
    health_rows = []
    for period, group in items.groupby(items["date"].dt.to_period("M")):
        health_rows.append({"Period": str(period), "score": _health_score(group)})
    dash_health = pd.DataFrame(health_rows).sort_values("Period").reset_index(drop=True)
    dash_health.to_sql("dash_groceries_health", con, if_exists="replace", index=False)

    # dash_groceries_top_articles — most purchased items
    top_articles = (
        items[items["price_chf"] > 0]
        .groupby(["article", "category_main"], as_index=False)
        .agg(
            count=("price_chf", "count"),
            total_chf=("price_chf", "sum"),
            avg_chf=("price_chf", "mean"),
        )
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )
    top_articles.to_sql(
        "dash_groceries_top_articles", con, if_exists="replace", index=False
    )
