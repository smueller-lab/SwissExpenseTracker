# -*- coding: utf-8 -*-
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
from Pipeline.config import *
from Pipeline.cfg_cleaning import config
cfg = config()

# %%
# --- Init ---
dr = Drive()
fn = Filename()

# %%
pdf = pd.read_parquet(oj(dr.Use_Bank_ZKB, fn.Master_ZKB))

# %%
# group values for Cafe
s_Cat_Cafe = ['Café', 'Cafe', 'Bar & Cafe', 'Coffee Shop']
pdf.loc[pdf['category_second'].isin(s_Cat_Cafe), 'category_second'] = 'Cafe'

# group values for Bakery
s_Cat_Bakery = ['Bakery', 'Pastry Shop']
pdf.loc[pdf['category_second'].isin(s_Cat_Bakery), 'category_second'] = 'Bakery'
pdf.loc[pdf['category_second'] == 'Bakery', 'category_main'] = 'Restaurant' 

# group values for Bar
s_Cat_Bar = ['Pub', 'Bar']
pdf.loc[pdf['category_second'].isin(s_Cat_Bar), 'category_second'] = 'Bar'

# group all other values for Restaurant under Restaurant under the 2nd category
pdf.loc[(pdf['category_main'] == 'Restaurant') & (~pdf['category_second'].isin(s_Cat_Cafe + s_Cat_Bakery + s_Cat_Bar)), 'category_second'] = 'Restaurant'

# set work place as Cafeteria because it's a collection of meals during one month
pdf.loc[(pdf['category_second'] == 'Restaurant') & (pdf['Merchant'] == cfg.nm_Work1), cfg.snm_Category] = ['Restaurant', 'Cafeteria']

# clean Merchant
pdf.loc[pdf['Merchant'].str.contains(cfg.nm_GroceryShop2), 'Merchant'] = cfg.nm_GroceryShop2

# clean Spa
pdf.loc[(pdf['Merchant'] == cfg.nm_CompanySpaMarathon) & (pdf['Date'] == cfg.Date_SpaMarathon), ['Merchant', 'category_main', 'category_second']] = [cfg.nm_SpaMarathon, 'Entertainment', 'Spa']

# group similar categories together
pdf.loc[pdf['category_main'].str.contains('Public Service', case=False, na=False), 'category_main'] = 'Public Service'
pdf.loc[pdf['Merchant'] == cfg.nm_HotelKisok, 'category_main'] = 'Groceries'
pdf.loc[pdf['category_second'] == 'Rental', 'category_main'] = 'Travel'
pdf['category_main'] = pdf['category_main'].replace({
    'Personal Care': 'Healthcare',
    'Hausing': 'Housing',
    'Health': 'Healthcare',
    'Financial': 'Finance',
    'Telecom': 'Telecommunication'
})

# clean 3rd pillar
pdf.loc[
    (pdf['Merchant'].str.contains(cfg.nm_FullName)) & 
    (pdf['Date'].isin(pd.to_datetime(cfg.s_Date_Investing))),
    ['Merchant', 'category_main', 'category_second']
] = [cfg.nm_ThirdPillar, 'Investing', 'Third pillar']

# %%
pdf.to_parquet(oj(dr.Use_Bank_ZKB, fn.Master_ZKB))
