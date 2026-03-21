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
pdf_Sport = pdf.loc[(pdf['transaction_type'] == 'expense') & (pdf['category_main'] == 'Sport')].reset_index(drop=True)
pdf_Sport['Year'] = pdf_Sport['Date'].dt.year
pdf_Sport['MonthYear'] = pdf_Sport['Date'].dt.to_period('M')

pdf_Sport = pdf_Sport[~pdf_Sport['category_second'].isin(['Sports Facility', 'Unknown', 'Sports Administration', 'Sports services'])].copy()

# %%
pdf_Sport_Year = pdf_Sport.groupby(['Year', 'category_second'], as_index=False)['amount_CHF'].sum().rename(columns={'amount_CHF': 'Total', 'category_second': 'category_sport'})
pdf_Sport_Month = pdf_Sport.groupby(['MonthYear', 'category_second'], as_index=False)['amount_CHF'].sum().rename(columns={'amount_CHF': 'Total', 'category_second': 'category_sport'})

pdf_Sport = pd.concat([
    pdf_Sport_Year.assign(Freq='Yearly', Period=pdf_Sport_Year['Year'].astype(str)),
    pdf_Sport_Month.assign(Freq='Monthly', Period=pdf_Sport_Month['MonthYear'].astype(str)),
]).drop(columns=['MonthYear', 'Year'])

# %%
pdf_Sport.to_pickle(fdp.pth_table_Sport)
