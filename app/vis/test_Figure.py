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
from app.vis.Figure import Fig
fdp = FDP()

# %%
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)
pdf_Food = pd.read_pickle(fdp.pth_table_Food)
pdf_Grocery = pd.read_pickle(fdp.pth_table_Groceries)
pdf_Balance = pd.read_pickle(fdp.pth_table_Balance)

# %%
F = Fig()
fig = F.fig_BoxFood(pdf)

# %%
fig.show()
