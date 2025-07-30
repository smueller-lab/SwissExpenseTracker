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
import os
import pandas as pd
from datetime import datetime
from Pipeline.config import *
from Pipeline.libs import load_cache, save_cache, parse_Pipeline_args
from Pipeline.cfg_cleaning import config

# --- Load CLI arg for q_Redo ---
q_Redo = parse_Pipeline_args()

# --- Init ---
dr = Drive()
fn = Filename()
cfg = config()

# %%
pth_Data = oj(dr.Use_Viseca, fn.Master_Viseca)
pth_Cache = oj(dr.Rfn_Viseca_labelAI_cleaned, fn.Cache_Viseca)
vk_Cache = load_cache(pth_Cache, q_Redo)

sfn = [f for f in os.listdir(dr.Rfn_Viseca_labelAI_cleaned) if f.endswith('.pkl')]
sfn_process = sfn if q_Redo else [f for f in sfn if f not in vk_Cache]

# %%
spdf_new = []
vk_Cache_new = {}

for fn in sfn_process:
    pth = oj(dr.Rfn_Viseca_labelAI_cleaned, fn)
    mtime = os.path.getmtime(pth)

    # check if file has changed
    if vk_Cache.get(fn) != mtime or q_Redo:
        pdf = pd.read_pickle(pth)
        spdf_new.append(pdf)

    vk_Cache_new[fn] = mtime


# combine with existing cache data
if os.path.exists(pth_Data) and not q_Redo:
    pdf_cache = pd.read_parquet(pth_Data)
    pdf_new = pd.concat([pdf_cache] + spdf_new, ignore_index=True)
else:
    pdf_new = pd.concat(spdf_new, ignore_index=True)

pdf = pdf_new.drop_duplicates(subset='TransactionID')

# %%
# get Billing and CardFees transactions
pdf['Date_Valuta'] = pd.to_datetime(pdf['Date_Valuta'])
pdf['month_Valuta'] = pdf['Date_Valuta'].dt.to_period('M')
pdf.loc[(pdf['MerchantName'] == 'Viseca') & (pdf['amount_CHF'] == cfg.CardFees), 'q_CardFees'] = 1
pdf.loc[(pdf['MerchantName'] == 'Viseca') & (pdf['Details'] == 'Ihre Zahlung - Danke'), 'q_Billing'] = 1

# sort by Date_Valuta with Card Fees being always the top
pdf['sort_prio'] = (pdf['q_CardFees'] == 1).astype(int)
pdf = pdf.sort_values(by=['Date_Valuta', 'sort_prio'], ascending=[False, False])

# fill Date_billing
pdf['marker_billing'] = pdf['Date_Valuta'].where(pdf['q_CardFees'] == 1)
pdf['Date_billing'] = pdf['marker_billing'].ffill()

# set Date_billing of Billing = CardFees if they are in the same month
pdf_billingMap = pdf.loc[pdf['q_CardFees'] == 1].drop_duplicates('month_Valuta').set_index('month_Valuta')['Date_billing']
pdf.loc[pdf['q_Billing'] == 1, 'Date_billing'] = pdf.loc[pdf['q_Billing'] == 1, 'month_Valuta'].map(pdf_billingMap)

# validate if sum of transactions of one billing date always equals credit card payment of the same billing date
# TODO: Write UnitTest for that
pdf['amount_signed'] = pdf['amount_CHF']
pdf.loc[pdf['transaction_type'] == 'income', 'amount_signed'] *= -1 
mk = pdf['q_Billing'] != 1
billing_sums = (pdf[mk].groupby('Date_billing')['amount_signed'].sum().rename('Billing_Total'))
pdf['Billing_Total'] = pdf['Date_billing'].map(billing_sums)

# drop not needed columns
pdf = pdf.drop(columns=['q_CardFees', 'marker_billing', 'amount_signed', 'Billing_Total', 'q_Billing', 'month_Valuta', 'sort_prio'])

# %%
# pdf to parquet
date_fn = datetime.today().strftime('%Y%m%d')

pth_use_Master_v = oj(dr.Use_Viseca, f'Master_Viseca_{date_fn}.parquet')

pdf.to_parquet(pth_use_Master_v, engine='pyarrow', index=False)
pdf.to_parquet(pth_Data, engine='pyarrow', index=False)

# save cache
save_cache(pth_Cache, vk_Cache_new)
