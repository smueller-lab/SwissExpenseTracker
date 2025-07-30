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
from Pipeline.OpenAI import OpenAI_Bot
from Pipeline.libs import (
    load_TID_cache,
    save_TID_cache,
    process_viseca,
    label_transactions,
    parse_Pipeline_args,
    get_sfn_process
)

# --- Load CLI arg for q_Redo ---
q_Redo = parse_Pipeline_args()

# --- Init ---
dr = Drive()

# %%
pth_Cache = oj(dr.Raw_Viseca, 'cache_TID.json')

vk_Cache = {} if q_Redo else load_TID_cache(pth_Cache)
vk_Cache.setdefault('sfn_processed', [])
vk_Cache.setdefault('s_TID', [])
s_TID_processed = set(vk_Cache['s_TID'])
sfn_process = get_sfn_process(dr.Raw_Viseca, vk_Cache, q_Redo)

# %%
for fn in sfn_process:
    pth = oj(dr.Raw_Viseca, fn)
    pdf = pd.read_pickle(pth)

    # do some processing steps
    pdf = process_viseca(pdf)

    # filter based on actually happened transaction time
    pdf = pdf.sort_values(by=['Date_Action', 'Time_Action'], ascending=False).reset_index(drop=True)

    # Only keep data from TransactionID's which haven't been processed yet and therefore are not present in cache
    pdf_new = pdf if q_Redo else pdf[~pdf['TransactionID'].isin(s_TID_processed)]

    if pdf_new.empty:
        vk_Cache['sfn_processed'].append(fn)
        continue

    # label transactions with OpenAI_Bot()
    Bot = OpenAI_Bot()
    pdf_labelAI = label_transactions(pdf_new, Bot)

    # write to rfn
    pth_Refined = oj(dr.Rfn_Viseca_labelAI, fn)
    pdf_labelAI.to_pickle(pth_Refined)

    # update cache
    vk_Cache['s_TID'].extend(pdf_labelAI['TransactionID'].tolist())
    vk_Cache['sfn_processed'].append(fn)

# save cache
save_TID_cache(vk_Cache, pth_Cache)
