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
from app.config import FDP, VIS
fdp = FDP()
vis = VIS()

# %%
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)

# %%
# filter data for Groceries
pdf_groceries = pdf[pdf['category_main'] == 'Groceries'].reset_index(drop=True)
pdf_groceries.loc[pdf_groceries['Merchant'].str.contains('Denner'), 'Merchant'] = 'Denner'

# get month-year from Date
pdf_groceries['MonthYear'] = pdf_groceries['Date'].dt.to_period('M').dt.to_timestamp()

# subselect the relevant Merchants for Groceries
pdf_groceries = pdf_groceries[pdf_groceries['Merchant'].isin(vis.s_Merchant_Grocery)].reset_index(drop=True)

# sum expenses per Grocery Merchant and per MonthYear
pdf_groceries_month = pdf_groceries.groupby(['MonthYear', 'Merchant'])['amount_CHF'].sum().reset_index()

# join percentage from total and total Grocery expenses per Year
pdf_total = pdf_groceries_month.groupby('MonthYear')['amount_CHF'].transform('sum')
pdf_groceries_month['pct'] = pdf_groceries_month['amount_CHF'] / pdf_total * 100
pdf_groceries_month['Total'] = pdf_total

# %%
pdf_groceries_month.to_parquet(fdp.pth_app_GroceryMonth)
