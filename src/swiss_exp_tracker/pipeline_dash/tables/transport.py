from __future__ import annotations

import sqlite3

import pandas as pd

from swiss_exp_tracker.pipeline_dash.config import TRANSPORT_EXCLUDE_SECOND
from swiss_exp_tracker.pipeline_dash.config import TRANSPORT_HEATMAP_EXCLUDE_SECOND
from swiss_exp_tracker.pipeline_dash.config import TRANSPORT_MAIN_CATEGORIES


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    pdf = df[
        (df["transaction_type"] == "EXPENSE")
        & df["category_main"].isin(TRANSPORT_MAIN_CATEGORIES)
    ].copy()

    exclude_mask = (pdf["category_main"] == "Car") & pdf["category_second"].isin(
        TRANSPORT_EXCLUDE_SECOND
    )
    pdf = pdf[~exclude_mask].copy()

    pdf["Year"] = pdf["date"].dt.year
    pdf["Month_num"] = pdf["date"].dt.month
    pdf["category_transport"] = pdf["category_second"].fillna(pdf["category_main"])

    transport_sum = pdf.groupby(["Year", "category_transport"], as_index=False)[
        "amount"
    ].sum()
    transport_sum.to_sql("dash_transport", con, if_exists="replace", index=False)

    pdf_heatmap = pdf[
        ~pdf["category_second"].isin(TRANSPORT_HEATMAP_EXCLUDE_SECOND)
    ].copy()
    heatmap = pdf_heatmap.groupby(["Year", "Month_num"], as_index=False)["amount"].sum()
    heatmap["Month_name"] = pd.to_datetime(
        heatmap["Month_num"], format="%m"
    ).dt.month_name()
    heatmap.to_sql("dash_transport_heatmap", con, if_exists="replace", index=False)
