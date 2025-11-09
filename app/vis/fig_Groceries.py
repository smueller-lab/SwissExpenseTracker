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
import numpy as np
from app.config import FDP, VIS, config
from app.libs import get_ryAxis
import plotly.io as pio
import plotly.graph_objects as go
from ploty_template import myTemp
from sklearn.linear_model import LinearRegression
fdp = FDP()
vis = VIS()
cfg = config()
pio.templates.default = 'myTemp'

# %%
pdf = pd.read_parquet(fdp.pth_app_GroceryMonth)

# %%
fig_overall = go.Figure()

for Merchant in vis.s_Merchant_Grocery:
    group = pdf[pdf['Merchant'] == Merchant]
    fig_overall.add_trace(go.Bar(
        x=group['MonthYear'],
        y=group['amount_CHF'],
        name=Merchant,
        marker=dict(color=vis.vk_GroceryStore_col[Merchant])
    ))

ry_Axes = get_ryAxis(cfg.d_Tick_grocery, pdf['Total'], True)

fig_overall.update_layout(
    barmode='stack',
    yaxis=dict(
        dtick=cfg.d_Tick_grocery,
        range=ry_Axes,
        showline=True
    )
)

with open(fdp.pth_fig_GroceryBar, 'w') as f:
    f.write(fig_overall.to_json())

# %%
fig_bar = go.Figure()

for Merchant in vis.s_Merchant_Grocery:
    group = pdf[pdf['Merchant'] == Merchant]
    fig_bar.add_trace(go.Bar(
        x=group['MonthYear'],
        y=group['pct'],
        name=Merchant,
        marker=dict(color=vis.vk_GroceryStore_col[Merchant])
    ))

ry_Axes = [0, 100]

fig_bar.update_layout(
    barmode='stack',
    yaxis=dict(
        dtick=cfg.d_Tick_pct,
        range=ry_Axes,
        showline=True
    )
)

with open(fdp.pth_fig_GroceryBarPct, 'w') as f:
    f.write(fig_bar.to_json())

# %%
fig_corr = go.Figure()

for Merchant in vis.s_Merchant_Grocery:
    group = pdf[pdf['Merchant'] == Merchant]
    color = vis.vk_GroceryStore_col[Merchant]

    fig_corr.add_trace(go.Scatter(
        x=group['pct'],
        y=group['Total'],
        mode='markers',
        name=Merchant,
        marker=dict(color=color),
        legendgroup=Merchant,
        showlegend=True
    ))

    if len(group) >= 2:
        sx = group[['pct']].values
        sy = group['Total'].values

        model = LinearRegression().fit(sx, sy)
        x_range = np.linspace(sx.min(), sx.max(), 100)
        y_pred = model.predict(x_range.reshape(-1, 1))

        fig_corr.add_trace(go.Scatter(
            x=x_range,
            y=y_pred,
            mode='lines',
            line=dict(color=color, width=2),
            name=Merchant,
            legendgroup=Merchant,
            showlegend=False
        ))

ry_Axes = get_ryAxis(cfg.d_Tick_grocery, pdf['Total'], True)
rx_Axes = get_ryAxis(10, pdf['pct'], True)

fig_corr.update_layout(
    yaxis=dict(
        dtick=cfg.d_Tick_grocery,
        range=ry_Axes,
        showline=True
    ),
    xaxis=dict(
        dtick=10,
        range=rx_Axes,
        showline=True
    )
)

with open(fdp.pth_fig_GroceryLR, 'w') as f:
    f.write(fig_corr.to_json())
