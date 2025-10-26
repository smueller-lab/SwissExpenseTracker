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
from app.config import FDP, config
import plotly.io as pio
import plotly.graph_objects as go
from ploty_template import myTemp
from app.libs import get_ryAxis, get_rxAxis_Date
fdp = FDP()
cfg = config()

pio.templates.default = "myTemp"

# %%
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)

# %%
pdf_Balance = pdf.groupby('Date', group_keys=False).last().reset_index()
pdf_Balance['Date'] = pd.to_datetime(pdf_Balance['Date'])

Date_1YearAgo = pdf_Balance['Date'].max() - pd.DateOffset(years=1)
pdf_Balance = pdf_Balance[pdf_Balance['Date'] >= Date_1YearAgo]

# %%
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=pdf_Balance['Date'],
    y=pdf_Balance['Balance_CHF'],
    mode='markers',
    name='Balance CHF'
))

ry_Axes = get_ryAxis(cfg.d_Tick_balance, pdf_Balance['Balance_CHF'])
s_tick_val, s_tick_text, format_Date = get_rxAxis_Date(pdf_Balance['Date'])

fig.update_layout(
    yaxis=dict(
        dtick=cfg.d_Tick_balance,
        range=ry_Axes,
        showline=True,
        linecolor='white'
    ),
    xaxis=dict(
        tickvals=s_tick_val,
        ticktext=s_tick_text,
        range=[pdf_Balance['Date'].min() - pd.Timedelta(days=3), s_tick_val[-1]],
        tickformat=format_Date,
        showline=True,
        linecolor='white',
    )
)

with open(fdp.pth_fig_BalancePerDay, "w") as f:
    f.write(fig.to_json())
