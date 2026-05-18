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
from app.config import FDP

fdp = FDP()

# %%
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)

# %%
# get month
pdf["Month"] = pd.to_datetime(pdf["Date"]).dt.to_period("M")

# sum expenses and income per month
pdf_ExpIncMonth = (
    pdf.groupby(["transaction_type", "Month"])["amount_CHF"].sum().reset_index()
)
pdf_ExpIncMonth = pdf_ExpIncMonth.pivot_table(
    index="Month", columns="transaction_type", values="amount_CHF"
).reset_index()
pdf_ExpIncMonth.columns.name = None

# calculate difference
pdf_ExpIncMonth["NetBalance"] = pdf_ExpIncMonth["income"] - pdf_ExpIncMonth["expense"]

# convert Month to string
pdf_ExpIncMonth = pdf_ExpIncMonth.sort_values(by="Month", ascending=False)
pdf_ExpIncMonth["Month"] = pdf_ExpIncMonth["Month"].astype(str)

pdf_ExpIncMonth.to_pickle(fdp.pth_table_NetBalanceMonth)
