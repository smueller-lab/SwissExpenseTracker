from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

import app.vis.ploty_template  # noqa: F401  # registers myTemp Plotly template

from app.config import VIS
from app.config import config
from app.libs import get_heightFigure
from app.libs import get_rxAxis_Date
from app.libs import get_ryAxis


vis = VIS()
cfg = config()
pio.templates.default = "myTemp"


class Fig:
    def __init__(self) -> None:
        self.vk_Margin = pio.templates["myTemp"].layout.margin

    def fig_BalancePerDay(self, pdf_Balance: pd.DataFrame) -> go.Figure:
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=pdf_Balance["Date"],
                y=pdf_Balance["Balance_CHF"],
                mode="markers",
                name="Balance CHF",
            )
        )

        ry_Axis = get_ryAxis(cfg.dTick_Balance, pdf_Balance["Balance_CHF"])
        height_Figure = get_heightFigure(
            ry_Axis, cfg.dTick_Balance, cfg.npixel_Balance, self.vk_Margin
        )
        s_tick_val, s_tick_text, format_Date = get_rxAxis_Date(pdf_Balance["Date"])

        fig.update_layout(
            yaxis={
                "dtick": cfg.dTick_Balance,
                "range": ry_Axis,
                "showline": True,
                "linecolor": "white",
            },
            xaxis={
                "tickvals": s_tick_val,
                "ticktext": s_tick_text,
                "range": [
                    pdf_Balance["Date"].min() - pd.Timedelta(days=3),
                    s_tick_val[-1],
                ],
                "tickformat": format_Date,
                "showline": True,
                "linecolor": "white",
            },
            height=height_Figure,
        )

        return fig

    def fig_BarGrocery(
        self, pdf_Grocery: pd.DataFrame, Freq: Literal["Monthly", "Yearly"]
    ) -> go.Figure:

        if Freq not in ["Monthly", "Yearly"]:
            raise ValueError(f"Invalid Freq={Freq}, Expected one of: Monthly, Yearly")

        fig = go.Figure()

        pdf_Grocery = pdf_Grocery[pdf_Grocery["Freq"] == Freq].reset_index(drop=True)

        for Merchant in vis.s_Merchant_Grocery:
            group = pdf_Grocery[pdf_Grocery["Merchant"] == Merchant]
            fig.add_trace(
                go.Bar(
                    x=group["Period"],
                    y=group["total_CHF"],
                    name=Merchant,
                    marker={"color": vis.vk_GroceryStore_col[Merchant]},
                )
            )

        dTick_Grocery = cfg.vk_dTick_Grocery[Freq]
        npixel_Grocery = cfg.vk_npixel_Grocery[Freq]
        ry_Axis = get_ryAxis(dTick_Grocery, pdf_Grocery["totalPeriod_CHF"], True)

        height_Figure = get_heightFigure(
            ry_Axis, dTick_Grocery, npixel_Grocery, self.vk_Margin
        )

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": dTick_Grocery, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_BarGrocery_pct(
        self, pdf_Grocery: pd.DataFrame, Freq: Literal["Monthly", "Yearly"]
    ) -> go.Figure:

        if Freq not in ["Monthly", "Yearly"]:
            raise ValueError(f"Invalid Freq={Freq}, Expected one of: Monthly, Yearly")

        fig = go.Figure()

        pdf_Grocery = pdf_Grocery[pdf_Grocery["Freq"] == Freq].reset_index(drop=True)

        for Merchant in vis.s_Merchant_Grocery:
            group = pdf_Grocery[pdf_Grocery["Merchant"] == Merchant]
            fig.add_trace(
                go.Bar(
                    x=group["Period"],
                    y=group["pct"],
                    name=Merchant,
                    marker={"color": vis.vk_GroceryStore_col[Merchant]},
                )
            )

        height_Figure = get_heightFigure(
            cfg.ry_Axis_Pct, cfg.dTick_Pct, cfg.npixel_Pct, self.vk_Margin
        )

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": cfg.dTick_Pct, "range": cfg.ry_Axis_Pct, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_BoxGrocery(self, pdf: pd.DataFrame) -> go.Figure:

        fig = go.Figure()

        stick_Text = []
        stick_Val = []

        for i, Merchant in enumerate(vis.s_Merchant_Grocery):
            pdf_Merchant = pdf[pdf["Merchant"] == Merchant].reset_index(drop=True)

            fig.add_trace(
                go.Box(
                    y=pdf_Merchant["amount_CHF"],
                    name=Merchant,
                    marker={"color": vis.vk_GroceryStore_col[Merchant]},
                )
            )

            stick_Text.append(f"{Merchant} (n={len(pdf_Merchant)})")
            stick_Val.append(i)

        pdf_Merchant = pdf[pdf["Merchant"].isin(vis.s_Merchant_Grocery)].reset_index(
            drop=True
        )
        dTick_Grocery = cfg.vk_dTick_Grocery["Visit"]
        npixel_Grocery = cfg.vk_npixel_Grocery["Visit"]
        ry_Axis = get_ryAxis(dTick_Grocery, pdf_Merchant["amount_CHF"], True)
        height_Figure = get_heightFigure(
            ry_Axis, dTick_Grocery, npixel_Grocery, self.vk_Margin
        )

        fig.update_layout(
            xaxis={"tickmode": "array", "tickvals": stick_Val, "ticktext": stick_Text},
            yaxis={"dtick": dTick_Grocery, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_BarFood(
        self, pdf_Food: pd.DataFrame, Freq: Literal["Monthly", "Yearly"]
    ) -> go.Figure:

        if Freq not in ["Monthly", "Yearly"]:
            raise ValueError(f"Invalid Freq={Freq}, Expected one of: Monthly, Yearly")

        fig = go.Figure()

        pdf_Food = pdf_Food[pdf_Food["Freq"] == Freq].reset_index(drop=True)
        s_Category_sort = (
            pdf_Food.groupby("category_second")["total_CHF"]
            .sum()
            .sort_values(ascending=False)
            .index.tolist()
        )

        for Category in s_Category_sort:
            group = pdf_Food[pdf_Food["category_second"] == Category]
            fig.add_trace(
                go.Bar(
                    x=group["Period"],
                    y=group["total_CHF"],
                    name=Category,
                    marker={"color": vis.vk_Food_col[Category]},
                )
            )

        dTick_Food = cfg.vk_dTick_Food[Freq]
        npixel_Food = cfg.vk_npixel_Food[Freq]
        ry_Axis = get_ryAxis(dTick_Food, pdf_Food["totalPeriod_CHF"], True)

        height_Figure = get_heightFigure(
            ry_Axis, dTick_Food, npixel_Food, self.vk_Margin
        )

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": dTick_Food, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_BoxFood(self, pdf: pd.DataFrame) -> go.Figure:

        fig = go.Figure()

        stick_Text = []
        stick_Val = []

        for i, Category in enumerate(vis.s_Category_Food):
            pdf_Category = pdf[pdf["category_second"] == Category].reset_index(
                drop=True
            )

            fig.add_trace(
                go.Box(
                    y=pdf_Category["amount_CHF"],
                    name=Category,
                    marker={"color": vis.vk_Food_col[Category]},
                )
            )

            stick_Text.append(f"{Category} (n={len(pdf_Category)})")
            stick_Val.append(i)

        pdf_Category = pdf[
            pdf["category_second"].isin(vis.s_Category_Food)
        ].reset_index(drop=True)
        dTick_Food = cfg.vk_dTick_Food["Visit"]
        npixel_Food = cfg.vk_npixel_Food["Visit"]
        ry_Axis = get_ryAxis(dTick_Food, pdf_Category["amount_CHF"], True)
        height_Figure = get_heightFigure(
            ry_Axis, dTick_Food, npixel_Food, self.vk_Margin
        )

        fig.update_layout(
            xaxis={"tickmode": "array", "tickvals": stick_Val, "ticktext": stick_Text},
            yaxis={"dtick": dTick_Food, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_DonutCategoryMain(self, pdf_CatMain: pd.DataFrame) -> go.Figure:
        fig = go.Figure(
            go.Pie(
                labels=pdf_CatMain["category_main"],
                values=pdf_CatMain["amount_CHF"],
                hole=0.4,
                textinfo="percent+label",
                textfont={"size": 12},
                pull=[0.02] * len(pdf_CatMain),
                domain={"x": [0.0, 0.9], "y": [0.0, 1.0]},
            )
        )

        fig.update_layout(showlegend=False)

        return fig

    def fig_BarVacation(self, pdf_Vacation: pd.DataFrame) -> go.Figure:
        fig = go.Figure()

        z_YearExpense = pdf_Vacation.groupby("Year")["Total"].sum()

        for Category in pdf_Vacation["category_second"].unique():
            group = pdf_Vacation[pdf_Vacation["category_second"] == Category]
            fig.add_trace(
                go.Bar(
                    x=group["Year"],
                    y=group["Total"],
                    name=Category,
                )
            )

        ry_Axis = get_ryAxis(cfg.dTick_Vacation, z_YearExpense, True)
        height_Figure = get_heightFigure(
            ry_Axis, cfg.dTick_Vacation, cfg.npixel_Vacation, self.vk_Margin
        )

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": cfg.dTick_Vacation, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_BarYearlyByCategory(
        self,
        pdf: pd.DataFrame,
        col_catgeory: str,
        col_amount: str,
        dTick: float,
        npixel: float,
    ) -> go.Figure:
        fig = go.Figure()

        z_YearExpense = pdf.groupby("Year")[col_amount].sum()

        for Category in pdf[col_catgeory].unique():
            group = pdf[pdf[col_catgeory] == Category]
            fig.add_trace(
                go.Bar(
                    x=group["Year"],
                    y=group[col_amount],
                    name=Category,
                )
            )

        ry_Axis = get_ryAxis(dTick, z_YearExpense, True)
        height_Figure = get_heightFigure(ry_Axis, dTick, npixel, self.vk_Margin)

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": dTick, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_BarFreqByCategory(
        self,
        pdf: pd.DataFrame,
        col_catgeory: str,
        col_amount: str,
        Freq: Literal["Monthly", "Yearly"],
        dTick: float,
        npixel: float,
    ) -> go.Figure:
        fig = go.Figure()

        pdf_Freq = pdf[pdf["Freq"] == Freq].reset_index(drop=True)

        # sum per period
        z_FreqExpense = pdf_Freq.groupby(["Period"])[col_amount].sum()

        # determine stacking order
        pdf_grouped = (
            pdf_Freq.groupby([col_catgeory, "Period"])[col_amount]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        z_category_totals = (
            pdf_grouped.groupby(col_catgeory)[col_amount]
            .sum()
            .sort_values(ascending=False)
        )
        s_category_orderstack_order = z_category_totals.index.tolist()

        for Category in s_category_orderstack_order:
            group = pdf_Freq[pdf_Freq[col_catgeory] == Category]
            fig.add_trace(
                go.Bar(
                    x=group["Period"],
                    y=group[col_amount],
                    name=Category,
                    marker={"color": vis.vk_Sport_col[Category]},
                )
            )

        ry_Axis = get_ryAxis(dTick, z_FreqExpense, True)
        height_Figure = get_heightFigure(ry_Axis, dTick, npixel, self.vk_Margin)

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": dTick, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_HeatmapMonthly(self, pdf: pd.DataFrame) -> go.Figure:
        s_month_order = [
            pdf[pdf["Month_num"] == i]["Month_name"].values[0] for i in range(1, 13)
        ]
        pdf_pivot = pdf.pivot(index="Year", columns="Month_name", values="amount_CHF")
        pdf_pivot = pdf_pivot[s_month_order]

        fig = go.Figure(
            go.Heatmap(
                z=pdf_pivot.values,
                x=pdf_pivot.columns,
                y=pdf_pivot.index,
                colorscale="RdYlGn_r",
                colorbar={"title": "CHF"},
                showscale=True,
                hovertemplate="Year: %{y}<br>Month: %{x}<br>Cost: %{z} CHF<extra></extra>",
            )
        )

        fig.update_layout(xaxis={"scaleanchor": "y"}, yaxis_autorange="reversed")

        return fig

    def fig_CategoryCorrelation(
        self,
        pdf: pd.DataFrame,
        col_category: str,
        Period: Literal["Month", "Week"],
        Year: int | None = None,
    ) -> go.Figure:

        df = pdf.copy()

        if Year is not None:
            df = df[df["Year"] == Year].reset_index(drop=True)

        df_pivot = df.pivot_table(
            index=Period,
            columns=col_category,
            values="amount_CHF",
            aggfunc="sum",
            fill_value=0,
        ).reset_index(drop=True)

        df_pivot = df_pivot.loc[(df_pivot != 0).any(axis=1)]
        corr = df_pivot.corr()
        np.fill_diagonal(corr.values, 0)

        # add text in cells with correlation values where corr is > 0.5 or < -0.5
        threshold_text = 0.5
        text = corr.copy().values.astype(str)
        text_mask = np.abs(corr.values) < threshold_text
        text[text_mask] = ""
        text = np.where(text != "", np.round(corr.values, 2).astype(str), "")

        fig = go.Figure(
            go.Heatmap(
                z=corr.values,
                x=corr.columns,
                y=corr.index,
                colorscale=[[0.0, "#b2182b"], [0.5, "#f7f7f7"], [1.0, "#2166ac"]],
                showscale=False,
                zmin=-1,
                zmax=1,
                text=text,
                texttemplate="%{text}",
                textfont={"color": "white", "size": 14},
            )
        )

        # set height based on n_categories
        n_categories = len(corr.index)
        npixel_row = 35
        npixel_min = 300
        npixel_max = 1200
        height_figure = min(max(n_categories * npixel_row, npixel_min), npixel_max)

        fig.update_layout(
            margin={"l": self.vk_Margin["l"] + 80, "b": self.vk_Margin["b"] + 70},
            height=height_figure,
        )

        return fig
