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
pdf['Year'] = pdf['Date'].dt.year
pdf['Month'] = pdf['Date'].dt.month
pdf['Week'] = pdf['Date'].dt.isocalendar().week    

# %%
z_category_count = pdf['category_second'].value_counts()
z_category_keep = z_category_count[z_category_count >= 10].index

pdf_CatCorr = pdf[pdf['category_second'].isin(z_category_keep)].reset_index(drop=True)

# drop NA category
pdf_CatCorr = pdf_CatCorr[pdf_CatCorr['category_second'] != 'NA'].reset_index(drop=True)

# drop useless categories
pdf_CatCorr = pdf_CatCorr[~pdf_CatCorr['category_second'].isin(['Card Fees', 'Cloud Services'])].reset_index(drop=True)

# %%
pdf_CatCorr.to_pickle(fdp.pth_table_CategoryCorrelation)
