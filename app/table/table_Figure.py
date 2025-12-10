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
# Balance
pdf_Balance = pdf.groupby('Date', group_keys=False).last().reset_index()
pdf_Balance['Date'] = pd.to_datetime(pdf_Balance['Date'])
Date_1YearAgo = pdf_Balance['Date'].max() - pd.DateOffset(years=1)
pdf_Balance = pdf_Balance[pdf_Balance['Date'] >= Date_1YearAgo].reset_index(drop=True)
pdf_Balance.to_pickle(fdp.pth_table_Balance)

# %%
# Groceries
pdf_Groceries = pdf[pdf['category_main'] == 'Groceries'].reset_index(drop=True)

# get month-year from Date
pdf_Groceries['Year'] = pdf_Groceries['Date'].dt.year
pdf_Groceries['MonthYear'] = pdf_Groceries['Date'].dt.to_period('M').dt.to_timestamp()

# subselect the relevant Merchants for Groceries
pdf_Groceries = pdf_Groceries[pdf_Groceries['Merchant'].isin(vis.s_Merchant_Grocery)].reset_index(drop=True)

pdf_Monthly = pdf_Groceries.groupby(['MonthYear', 'Merchant'], as_index=False)['amount_CHF'].sum().rename(columns={'amount_CHF': 'total_CHF'})
pdf_Monthly['totalPeriod_CHF'] = pdf_Monthly.groupby('MonthYear')['total_CHF'].transform('sum')
pdf_Monthly['pct'] = pdf_Monthly['total_CHF'] / pdf_Monthly['totalPeriod_CHF'] * 100

pdf_Yearly = pdf_Groceries.groupby(['Year', 'Merchant'], as_index=False)['amount_CHF'].sum().rename(columns={'amount_CHF': 'total_CHF'})
pdf_Yearly['totalPeriod_CHF'] = pdf_Yearly.groupby('Year')['total_CHF'].transform('sum')
pdf_Yearly['pct'] = pdf_Yearly['total_CHF'] / pdf_Yearly['totalPeriod_CHF'] * 100

pdf_Groceries = pd.concat([
    pdf_Monthly.assign(Freq='Monthly', Period=pdf_Monthly['MonthYear'].astype(str)),
    pdf_Yearly.assign(Freq='Yearly', Period=pdf_Yearly['Year'].astype(str)),
]).drop(columns=['MonthYear', 'Year'])

pdf_Groceries.to_pickle(fdp.pth_table_Groceries)

# %%
# Food
pdf_Groceries = pdf[pdf['category_main'] == 'Groceries'].reset_index(drop=True)
pdf_Restaurant = pdf[pdf['category_main'] == 'Restaurant'].reset_index(drop=True)
pdf_Groceries['category_second'] = 'Groceries'

pdf_Food = pd.concat([pdf_Groceries, pdf_Restaurant], ignore_index=True)
pdf_Food['MonthYear'] = pdf_Food['Date'].dt.to_period('M').dt.to_timestamp()
pdf_Food['Year'] = pdf_Food['Date'].dt.year

pdf_Monthly = pdf_Food.groupby(['MonthYear', 'category_second'], as_index=False)['amount_CHF'].sum().rename(columns={'amount_CHF': 'total_CHF'})
pdf_Monthly['totalPeriod_CHF'] = pdf_Monthly.groupby('MonthYear')['total_CHF'].transform('sum')
pdf_Monthly['pct'] = pdf_Monthly['total_CHF'] / pdf_Monthly['totalPeriod_CHF'] * 100

pdf_Yearly = pdf_Food.groupby(['Year', 'category_second'], as_index=False)['amount_CHF'].sum().rename(columns={'amount_CHF': 'total_CHF'})
pdf_Yearly['totalPeriod_CHF'] = pdf_Yearly.groupby('Year')['total_CHF'].transform('sum')
pdf_Yearly['pct'] = pdf_Yearly['total_CHF'] / pdf_Yearly['totalPeriod_CHF'] * 100

pdf_Food = pd.concat([
    pdf_Monthly.assign(Freq='Monthly', Period=pdf_Monthly['MonthYear'].astype(str)),
    pdf_Yearly.assign(Freq='Yearly', Period=pdf_Yearly['Year'].astype(str)),
]).drop(columns=['MonthYear', 'Year'])

pdf_Food.to_pickle(fdp.pth_table_Food)
