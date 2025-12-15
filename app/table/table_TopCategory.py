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

# Top Category
pdf_Expenses = pdf[(pdf['transaction_type'] == 'expense') & ~(pdf['category_main'].isin(['Housing', 'Government']))].copy()
Date_max = pdf['Date'].max()

if Date_max.is_month_end:
    MonthLast = Date_max.to_period('M')
else:
    MonthLast = (Date_max - pd.offsets.MonthEnd(1)).to_period('M')

MonthPrev = MonthLast - 1
Month_Start12m = MonthLast - 11

pdf_Monthly = pdf_Expenses.groupby(['Month', 'category_main'], as_index=False)['amount_CHF'].sum()

pdf_Last = pdf_Monthly[pdf_Monthly['Month'] == MonthLast].rename(columns={'amount_CHF': 'amount_MonthLast'})
pdf_Prev = pdf_Monthly[pdf_Monthly['Month'] == MonthPrev].rename(columns={'amount_CHF': 'amount_MonthPrev'})
pdf_12m_avg = (
    pdf_Monthly[
        (pdf_Monthly['Month'] <= Month_Start12m) &
        (pdf_Monthly['Month'] <= MonthLast)
    ].groupby('category_main', as_index=False)['amount_CHF']
    .mean()
    .rename(columns={'amount_CHF': 'amount_AVG_12m'})
)

pdf_Comp = (
    pdf_Last[['category_main', 'amount_MonthLast']]
    .merge(pdf_Prev[['category_main', 'amount_MonthPrev']], on='category_main', how='left')
    .merge(pdf_12m_avg, on='category_main', how='left')
).sort_values(by='amount_MonthLast', ascending=False).reset_index(drop=True)

pdf_Comp['diff_prev_pct'] = (pdf_Comp['amount_MonthLast'] - pdf_Comp['amount_MonthPrev']) / pdf_Comp['amount_MonthPrev'] * 100
pdf_Comp['diff_12m_pct'] = (pdf_Comp['amount_MonthLast'] - pdf_Comp['amount_AVG_12m']) / pdf_Comp['amount_AVG_12m'] * 100

pdf_Comp['MonthLast'] = MonthLast

pdf_Comp.to_pickle(fdp.pth_table_TopCat)
