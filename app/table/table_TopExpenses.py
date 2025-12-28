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
pdf_TopExpenses = pdf[pdf['transaction_type'] == 'expense'].sort_values(by='amount_CHF', ascending=False)

# drop categories like Fried, Tax Services, and Finance
pdf_TopExpenses = pdf_TopExpenses[~pdf_TopExpenses['category_main'].isin([
    'Government', 'Finance', 'Friend', 'Housing', 'Hausing', 'Financial Services', 'Healthcare', 'Investing'
])].reset_index(drop=True)

# only select data from the last year so it's equivalent to BalancePerDay
Date_1YearAgo = pdf['Date'].max() - pd.DateOffset(years=1)
pdf_TopExpenses = pdf_TopExpenses[pdf_TopExpenses['Date'] >= Date_1YearAgo].copy()

scol_display = ['Date', 'amount_CHF', 'Merchant', 'category_main']
pdf_TopExpenses = pdf_TopExpenses[scol_display].head(20).copy()

pdf_TopExpenses.columns = [vis.vk_Variable_show[col] if col in vis.vk_Variable_show.keys() else col for col in pdf_TopExpenses.columns]

# %%
pdf_TopExpenses.to_pickle(fdp.pth_table_TopExpenses)
