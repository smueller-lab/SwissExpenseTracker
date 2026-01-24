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
from app.vis.figure import Fig
fdp = FDP()

# %%
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)

pdf_TransportHeatmap = pd.read_pickle(fdp.pth_table_TransportHeatmap)

# %%
F = Fig()

fig = F.fig_HeatmapMonthly(pdf_TransportHeatmap)

fig.show()
