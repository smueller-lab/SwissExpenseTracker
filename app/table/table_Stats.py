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
pdf['Month'] = pd.to_datetime(pdf['Date']).dt.to_period('M')
pdf['Year'] = pd.to_datetime(pdf['Date']).dt.year

pdf_Month = pdf.groupby(['Month', 'transaction_type'])['amount_CHF'].sum().unstack(fill_value=0)
pdf_Month['Balance_netMonth'] = pdf_Month.get('income', 0) - pdf_Month.get('expense', 0)
pdf_Month = pdf_Month.reset_index()

pdf_Year = pdf.groupby(['Year', 'transaction_type'])['amount_CHF'].sum().unstack(fill_value=0)
pdf_Year['Balance_netYear'] = pdf_Year.get('income', 0) - pdf_Year.get('expense', 0)
pdf_Year = pdf_Year.sort_values(by='Year').reset_index()

# %%
vk_Stats = {}

vk_Stats.update({
    'Balance_current': pdf.sort_values(by='Date')['Balance_CHF'].iloc[-1],
    'Balance_net_3months': pdf_Month['Balance_netMonth'].tail(3).mean(),
    'Balance_net_12months': pdf_Month['Balance_netMonth'].tail(12).mean(),
    'Balance_net_currentYear': pdf_Year['Balance_netYear'].iloc[-1]
})

z_StatsTable = pd.Series(vk_Stats)

# %%
z_StatsTable.to_pickle(fdp.pth_table_Stats)
