# %%
import pandas as pd
from datetime import datetime
from Pipeline.config import *
from Pipeline.cfg_cleaning import config

# --- Init ---
dr = Drive()
fn = Filename()
cfg = config()

# %%
pth_Master = oj(dr.Rfn_Debit_Master, fn.Master_Debit)
pdf = pd.read_parquet(pth_Master)

# %%
# count how often each combination of category_main, category_second appears for every nm_subject
pdf_count = pdf.groupby(['nm_subject', 'category_main', 'category_second']).size().reset_index(name='count')

# for each nm_subject get the combination with the highest count
pdf_dict = pdf_count.sort_values('count', ascending=False).drop_duplicates(subset='nm_subject').reset_index(drop=True)
pdf_dict = pdf_dict[pdf_dict['count'] > 2].copy()
pdf_dict = pdf_dict[~pdf_dict['nm_subject'].isin([cfg.nm_Work1])]

# merge counted top categories back to pdf
pdf = pdf.merge(pdf_dict, how='left', on='nm_subject', suffixes=('_old', ''))

# fillna in new categories where there is not a high enough count yet
pdf['category_main'] = pdf['category_main'].fillna(pdf['category_main_old'])
pdf['category_second'] = pdf['category_second'].fillna(pdf['category_second_old'])

# drop not needed columns
pdf = pdf.drop(columns=['category_main_old', 'category_second_old', 'count'])

# %%
date_fn = datetime.today().strftime("%Y%m%d")

pth_use_Master_v = oj(dr.Use_Debit, f'Master_DebitCard_{date_fn}.parquet')
pth_use_Master = oj(dr.Use_Debit, fn.Master_Debit)
pdf.to_parquet(pth_use_Master_v)
pdf.to_parquet(pth_use_Master)