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
from Pipeline.libs import (
    load_cache,
    save_cache,
    parse_Pipeline_args,
    get_sfn_process
)

# --- Load CLI arg for q_Redo ---
q_Redo = parse_Pipeline_args()

# --- Init ---
dr = Drive()
fn = Filename()
# %%
pth_Cache = oj(dr.Lnd_Viseca, fn.Cache_Viseca)
vk_Cache = load_cache(pth_Cache, q_Redo)
sfn_process = get_sfn_process(dr.Lnd_Viseca, vk_Cache, q_Redo)

# %%
for fn in sfn_process:
    pth = oj(dr.Lnd_Viseca, fn)
    pdf = pd.read_csv(pth)

    fn_output = fn.replace('.csv', '.pkl')
    pth_output = oj(dr.Raw_Viseca, fn_output)
    pdf.to_pickle(pth_output)

    # update cache
    vk_Cache['sfn_processed'].append(fn)

save_cache(pth_Cache, vk_Cache)
