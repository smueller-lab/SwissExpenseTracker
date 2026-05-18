# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: venv
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
import numpy as np
from app.config import FDP

fdp = FDP()

# %%
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)

# %%
pdf = pdf[(pdf["transaction_type"] == "expense")].reset_index(drop=True)
pdf = pdf.loc[
    (pdf["category_main"].isin(["Gas station", "Transport", "Parking"]))
    | ((pdf["category_main"] == "Insurance") & (pdf["category_second"] == "Car"))
]
pdf["Year"] = pd.to_datetime(pdf["Date"]).dt.year
pdf["Month_num"] = pd.to_datetime(pdf["Date"]).dt.month

# %%
# Gas station, Parking, Train, Bus, Ferry, Car Dealership, Car sharing
s_transport_subcategory = [
    "Gas station",
    "Parking",
    "Train",
    "Bus",
    "Ferry",
    "Car Dealership",
    "Car sharing",
    "Insurance",
]

pdf_transport = pdf.copy()

pdf_transport["category_transport"] = np.where(
    pdf["category_main"].isin(s_transport_subcategory),
    pdf["category_main"],
    np.where(
        pdf["category_second"].isin(s_transport_subcategory),
        pdf["category_second"],
        None,
    ),
)

pdf_transport = pdf_transport[
    (
        pdf_transport["category_main"].isin(s_transport_subcategory)
        | pdf_transport["category_second"].isin(s_transport_subcategory)
    )
].copy()

# remove car purchase from transport analytics
pdf_transport = pdf_transport[
    ~(
        (pdf_transport["Date"] == "2023-08-15")
        & (pdf_transport["category_transport"] == "Car Dealership")
    )
].copy()

# groupby category_transport and sum expenses
pdf_transport_sum = pdf_transport.groupby(
    ["Year", "category_transport"], as_index=False
)["amount_CHF"].sum()

# %%
# Monthly Heatmap dataframe
s_transport_subcategory_heatmap = [
    "Gas station",
    "Parking",
    "Train",
    "Bus",
    "Ferry",
    "Car sharing",
]

pdf_transport_heatmap = pdf.copy()

pdf_transport_heatmap["category_transport"] = np.where(
    pdf["category_main"].isin(s_transport_subcategory_heatmap),
    pdf["category_main"],
    np.where(
        pdf["category_second"].isin(s_transport_subcategory_heatmap),
        pdf["category_second"],
        None,
    ),
)

pdf_transport_heatmap = pdf_transport_heatmap[
    (
        pdf_transport_heatmap["category_main"].isin(s_transport_subcategory_heatmap)
        | pdf_transport_heatmap["category_second"].isin(s_transport_subcategory_heatmap)
    )
].copy()

# remove car purchase from transport analytics
pdf_transport_heatmap = pdf_transport_heatmap[
    ~(
        (pdf_transport_heatmap["Date"] == "2023-08-15")
        & (pdf_transport_heatmap["category_transport"] == "Car Dealership")
    )
].copy()

# make heatmap
pdf_transport_heatmap = pdf_transport_heatmap.groupby(
    ["Year", "Month_num"], as_index=False
)["amount_CHF"].sum()
pdf_transport_heatmap["Month_name"] = pd.to_datetime(
    pdf_transport_heatmap["Month_num"], format="%m"
).dt.month_name()

# %%
pdf_transport_sum.to_pickle(fdp.pth_table_Transport)
pdf_transport_heatmap.to_pickle(fdp.pth_table_TransportHeatmap)
