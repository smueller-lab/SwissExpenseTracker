import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from app.vis.ploty_template import myTemp
import numpy as np
from app.config import VIS, config
from app.libs import get_ryAxis, get_rxAxis_Date
from typing import Literal
vis = VIS()
cfg = config()
pio.templates.default = 'myTemp'


class Fig:
    def __init__(self):
        pass


    def fig_BalancePerDay(self, pdf_Balance: pd.DataFrame):
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

        return fig
    

    def fig_BarGrocery(self, pdf_Grocery: pd.DataFrame, Freq: Literal['Monthly', 'Yearly']):

        if Freq not in ['Monthly', 'Yearly']:
            raise ValueError(
                f'Invalid Freq={Freq}, Expected one of: Monthly, Yearly' 
            )

        fig = go.Figure()

        pdf_Grocery = pdf_Grocery[pdf_Grocery['Freq'] == Freq].reset_index(drop=True)

        for Merchant in vis.s_Merchant_Grocery:
            group = pdf_Grocery[pdf_Grocery['Merchant'] == Merchant]
            fig.add_trace(go.Bar(
                x=group['Period'],
                y=group['total_CHF'],
                name=Merchant,
                marker=dict(color=vis.vk_GroceryStore_col[Merchant])
            ))

        dTick_Grocery = cfg.vk_dTick_Grocery[Freq]
        ry_Axes = get_ryAxis(dTick_Grocery, pdf_Grocery['totalPeriod_CHF'], True)

        fig.update_layout(
            barmode='stack',
            yaxis=dict(
                dtick=dTick_Grocery,
                range=ry_Axes,
                showline=True
            )
        )

        return fig
    

    def fig_BarGrocery_pct(self, pdf_Grocery: pd.DataFrame, Freq: Literal['Monthly', 'Yearly']):

        if Freq not in ['Monthly', 'Yearly']:
            raise ValueError(
                f'Invalid Freq={Freq}, Expected one of: Monthly, Yearly' 
            )
        
        fig = go.Figure()

        pdf_Grocery = pdf_Grocery[pdf_Grocery['Freq'] == Freq].reset_index(drop=True)
        
        for Merchant in vis.s_Merchant_Grocery:
            group = pdf_Grocery[pdf_Grocery['Merchant'] == Merchant]
            fig.add_trace(go.Bar(
                x=group['Period'],
                y=group['pct'],
                name=Merchant,
                marker=dict(color=vis.vk_GroceryStore_col[Merchant])
            ))

        ry_Axes = [0, 100]

        fig.update_layout(
            barmode='stack',
            yaxis=dict(
                dtick=cfg.d_Tick_pct,
                range=ry_Axes,
                showline=True
            )
        )

        return fig
    

    def fig_BoxGrocery(self, pdf: pd.DataFrame):

        fig = go.Figure()

        stick_Text = []
        stick_Val = []

        for i, Merchant in enumerate(vis.s_Merchant_Grocery):
            pdf_Merchant = pdf[pdf['Merchant'] == Merchant].reset_index(drop=True)

            fig.add_trace(go.Box(
                y=pdf_Merchant['amount_CHF'],
                name=Merchant,
                marker=dict(color=vis.vk_GroceryStore_col[Merchant]),
            ))

            stick_Text.append(f'{Merchant} (n={len(pdf_Merchant)})')
            stick_Val.append(i)

        dTick_Grocery = cfg.vk_dTick_Grocery['Visit']
        pdf_Merchant = pdf[pdf['Merchant'].isin(vis.s_Merchant_Grocery)].reset_index(drop=True)
        ry_Axes = get_ryAxis(dTick_Grocery, pdf_Merchant['amount_CHF'], True)

        fig.update_layout(
            xaxis=dict(
                tickmode='array',
                tickvals=stick_Val,
                ticktext=stick_Text
            ),
            yaxis=dict(
                dtick=dTick_Grocery,
                range=ry_Axes,
                showline=True
            )
        )

        return fig
    

    def fig_BarFood(self, pdf_Food: pd.DataFrame, Freq: Literal['Monthly', 'Yearly']):

        if Freq not in ['Monthly', 'Yearly']:
            raise ValueError(
                f'Invalid Freq={Freq}, Expected one of: Monthly, Yearly' 
            )
        
        fig = go.Figure()

        pdf_Food = pdf_Food[pdf_Food['Freq'] == Freq].reset_index(drop=True)
        s_Category_sort = pdf_Food.groupby('category_second')['total_CHF'].sum().sort_values(ascending=False).index.tolist()
        
        for Category in s_Category_sort:
            group = pdf_Food[pdf_Food['category_second'] == Category]
            fig.add_trace(go.Bar(
                x=group['Period'],
                y=group['total_CHF'],
                name=Category,
                marker=dict(color=vis.vk_Food_col[Category])
            ))

        dTick_Food = cfg.vk_dTick_Food[Freq]
        ry_Axes = get_ryAxis(dTick_Food, pdf_Food['totalPeriod_CHF'], True)

        fig.update_layout(
            barmode='stack',
            yaxis=dict(
                dtick=dTick_Food,
                range=ry_Axes,
                showline=True
            ),
            height=700
        )

        return fig
    

    def fig_BoxFood(self, pdf: pd.DataFrame):
        
        fig = go.Figure()

        stick_Text = []
        stick_Val = []

        for i, Category in enumerate(vis.s_Category_Food):
            pdf_Category = pdf[pdf['category_second'] == Category].reset_index(drop=True)

            fig.add_trace(go.Box(
                y=pdf_Category['amount_CHF'],
                name=Category,
                marker=dict(color=vis.vk_Food_col[Category])
            ))

            stick_Text.append(f'{Category} (n={len(pdf_Category)})')
            stick_Val.append(i)

        dTick_Food = cfg.vk_dTick_Food['Visit']
        pdf_Category = pdf[pdf['category_second'].isin(vis.s_Category_Food)].reset_index(drop=True)
        ry_Axes = get_ryAxis(dTick_Food, pdf_Category['amount_CHF'], True)

        fig.update_layout(
            xaxis=dict(
                tickmode='array',
                tickvals=stick_Val,
                ticktext=stick_Text
            ),
            yaxis=dict(
                dtick=dTick_Food,
                range=ry_Axes,
                showline=True
            )
        )

        return fig

