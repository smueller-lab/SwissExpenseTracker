# %%
import pandas as pd
import os
from Pipeline.config import *
from datetime import datetime

from Pipeline.libs import (
    load_cache,
    save_cache,
    extract_date,
    parse_Pipeline_args
)

# --- Load CLI arg for q_Redo ---
q_Redo = parse_Pipeline_args()

# --- Init ---
dr = Drive()
fn = Filename()

# %%
pth_Data = oj(dr.Rfn_Debit_Master, fn.Master_Debit)
pth_Cache = oj(dr.Rfn_Debit_labelAI_cleaned, fn.Cache_Debit)
vk_Cache = load_cache(pth_Cache, q_Redo)

# %%
spdf = []
vk_Cache_new = {}

for fn in os.listdir(dr.Rfn_Debit_labelAI_cleaned):
    if fn.endswith('.pkl'):
        pth = oj(dr.Rfn_Debit_labelAI_cleaned, fn)
        mtime = os.path.getmtime(pth)

        # check if file has changed
        if vk_Cache.get(fn) != mtime or q_Redo:
            pdf = pd.read_pickle(pth)
            
            date_fn = extract_date(fn)
            pdf['__Date_source'] = date_fn
            spdf.append(pdf)

        vk_Cache_new[fn] = mtime

# combine with existing cached data
if os.path.exists(pth_Data) and not q_Redo:
    pdf_cache = pd.read_parquet(pth_Data)
    spdf.append(pdf_cache)

# sort dataframe based on data source and index to gurantee that the data is in the right order
# important step to properly merge the Viseca data afterwards
pdf_new = pd.concat(spdf)
pdf_new['idx'] = pdf_new.index
pdf_new = pdf_new.sort_values(by=['__Date_source', 'idx'], ascending=[False, True])
pdf_new = pdf_new.reset_index(drop=True)
pdf_new = pdf_new.drop(columns=['__Date_source', 'idx'])

# pdf to parquet
date_fn = datetime.today().strftime("%Y%m%d")

pth_use_Master_v = oj(dr.Rfn_Debit_Master, f'Master_DebitCard_{date_fn}.parquet')

pdf_new.to_parquet(pth_use_Master_v, index=False)
pdf_new.to_parquet(pth_Data, index=False)

# save cache
save_cache(pth_Cache, vk_Cache_new)