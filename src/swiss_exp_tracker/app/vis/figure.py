from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]
import plotly.io as pio  # pyright: ignore[reportMissingTypeStubs]

import swiss_exp_tracker.app.vis.ploty_template  # pyright: ignore[reportUnusedImport] # noqa: F401  # registers myTemp Plotly template

from swiss_exp_tracker.app.config import VIS
from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.libs import get_heightFigure
from swiss_exp_tracker.app.libs import get_rxAxis_Date
from swiss_exp_tracker.app.libs import get_ryAxis


vis = VIS()
cfg = config()
pio.templates.default = "myTemp"


class Fig:
    def __init__(self) -> None:
        self.vk_Margin = pio.templates[
            "myTemp"
        ].layout.margin  # pyright: ignore[reportOptionalMemberAccess, reportAttributeAccessIssue]

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

    def fig_DonutByCategory(
        self,
        pdf: pd.DataFrame,
        col_category: str,
        col_amount: str,
        min_pct: float = 1.0,
    ) -> go.Figure:
        total = pdf[col_amount].sum()
        pct = pdf[col_amount] / total * 100 if total > 0 else pdf[col_amount] * 0

        text = [
            f"{label}<br>{p:.1f}%" if p >= min_pct else ""
            for label, p in zip(pdf[col_category], pct, strict=True)
        ]

        fig = go.Figure(
            go.Pie(
                labels=pdf[col_category],
                values=pdf[col_amount],
                hole=0.4,
                text=text,
                textinfo="text",
                textfont={"size": 12},
                pull=[0.02] * len(pdf),
                domain={"x": [0.0, 0.9], "y": [0.0, 1.0]},
                hovertemplate="%{label}<br>%{value:,.2f} CHF (%{percent})<extra></extra>",
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
        col_map: dict[str, str] | None = None,
    ) -> go.Figure:
        fig = go.Figure()

        if col_map is None:
            col_map = vis.vk_Sport_col

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
                    marker={"color": col_map.get(Category, "#95A5A6")},
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

    def fig_BarGroceryCat(
        self, pdf: pd.DataFrame, Freq: Literal["Monthly", "Yearly"]
    ) -> go.Figure:
        if Freq not in ["Monthly", "Yearly"]:
            raise ValueError(f"Invalid Freq={Freq}, Expected one of: Monthly, Yearly")

        fig = go.Figure()
        pdf_freq = pdf[pdf["Freq"] == Freq].reset_index(drop=True)

        xaxis: dict[str, object] = {}
        if Freq == "Monthly":
            # "2026-01-01" sorts lexicographically == chronologically
            sorted_raw = sorted(pdf_freq["Period"].unique())
            label_map: dict[str, str] = {
                p: pd.to_datetime(p).strftime("%b %Y") for p in sorted_raw
            }
            pdf_freq = pdf_freq.copy()
            pdf_freq["Period"] = pdf_freq["Period"].map(label_map)
            xaxis = {
                "categoryorder": "array",
                "categoryarray": [label_map[p] for p in sorted_raw],
            }
        else:
            sorted_years = sorted(pdf_freq["Period"].unique())
            xaxis = {
                "type": "category",
                "categoryorder": "array",
                "categoryarray": sorted_years,
            }

        cat_order = (
            pdf_freq.groupby("category_main")["total_CHF"]
            .sum()
            .sort_values(ascending=False)
            .index.tolist()
        )

        for cat in cat_order:
            group = pdf_freq[pdf_freq["category_main"] == cat]
            fig.add_trace(
                go.Bar(
                    x=group["Period"],
                    y=group["total_CHF"],
                    name=cat,
                    marker={"color": vis.vk_GroceryCat_col.get(cat, "#95A5A6")},
                )
            )

        dTick = cfg.vk_dTick_GroceryCat[Freq]
        npixel = cfg.vk_npixel_GroceryCat[Freq]
        total_per_period = pdf_freq.groupby("Period")["total_CHF"].sum()
        ry_Axis = get_ryAxis(dTick, total_per_period, True)
        height_Figure = get_heightFigure(ry_Axis, dTick, npixel, self.vk_Margin)

        fig.update_layout(
            barmode="stack",
            xaxis=xaxis,
            yaxis={"dtick": dTick, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )
        return fig

    @staticmethod
    def _color_shades(hex_color: str, n: int) -> list[str]:
        r, g, b = (
            int(hex_color[1:3], 16),
            int(hex_color[3:5], 16),
            int(hex_color[5:7], 16),
        )
        shades = []
        for i in range(n):
            t = 1.0 - 0.6 * (i / max(n - 1, 1))
            shades.append(
                f"#{round(r*t + 255*(1-t)):02x}"
                f"{round(g*t + 255*(1-t)):02x}"
                f"{round(b*t + 255*(1-t)):02x}"
            )
        return shades

    def fig_DonutGroceryCat(
        self,
        pdf_items: pd.DataFrame,
        category_main: str | None = None,
        category_detail: str | None = None,
    ) -> go.Figure:
        if category_main is None:
            grouped = (
                pdf_items[pdf_items["price_chf"] > 0]
                .groupby("category_main")["price_chf"]
                .sum()
                .sort_values(ascending=False)
                .reset_index()
            )
            labels = grouped["category_main"].tolist()
            values = grouped["price_chf"].tolist()
            colors = [vis.vk_GroceryCat_col.get(c, "#95A5A6") for c in labels]
        elif category_detail is None:
            grouped = (
                pdf_items[
                    (pdf_items["category_main"] == category_main)
                    & (pdf_items["price_chf"] > 0)
                ]
                .groupby("category_detail")["price_chf"]
                .sum()
                .sort_values(ascending=False)
                .reset_index()
            )
            labels = grouped["category_detail"].tolist()
            values = grouped["price_chf"].tolist()
            base = vis.vk_GroceryCat_col.get(category_main, "#95A5A6")
            colors = self._color_shades(base, len(labels))
        else:
            grouped = (
                pdf_items[
                    (pdf_items["category_main"] == category_main)
                    & (pdf_items["category_detail"] == category_detail)
                    & (pdf_items["price_chf"] > 0)
                ]
                .groupby("article")["price_chf"]
                .sum()
                .sort_values(ascending=False)
                .reset_index()
            )
            labels = grouped["article"].tolist()
            values = grouped["price_chf"].tolist()
            base = vis.vk_GroceryCat_col.get(category_main, "#95A5A6")
            colors = self._color_shades(base, len(labels))

        total = sum(values) if values else 1.0
        text = [
            f"{label}<br>{v / total * 100:.1f}%"
            for label, v in zip(labels, values, strict=True)
        ]

        fig = go.Figure(
            go.Pie(
                labels=labels,
                values=values,
                hole=0.4,
                text=text,
                textinfo="text",
                textfont={"size": 11},
                pull=[0.02] * len(labels),
                domain={"x": [0.0, 0.9], "y": [0.0, 1.0]},
                marker={"colors": colors},
                hovertemplate="%{label}<br>%{value:,.2f} CHF (%{percent})<extra></extra>",
            )
        )
        fig.update_layout(showlegend=False)
        return fig

    def fig_HealthIndex(self, pdf: pd.DataFrame) -> go.Figure:
        fig = go.Figure()

        fig.add_hrect(y0=70, y1=100, fillcolor="#4caf50", opacity=0.12, line_width=0)
        fig.add_hrect(y0=40, y1=70, fillcolor="#ff9800", opacity=0.12, line_width=0)
        fig.add_hrect(y0=0, y1=40, fillcolor="#ef5350", opacity=0.12, line_width=0)

        pdf = pdf.sort_values("Period").reset_index(drop=True)
        sorted_raw = pdf["Period"].tolist()
        x_labels = [pd.to_datetime(p).strftime("%b %Y") for p in sorted_raw]

        scores = [float(s) for s in pdf["score"]]
        marker_colors = [
            "#4caf50" if s >= 70 else ("#ff9800" if s >= 40 else "#ef5350")
            for s in scores
        ]

        fig.add_trace(
            go.Scatter(
                x=x_labels,
                y=pdf["score"],
                mode="lines+markers+text",
                text=[f"{s:.0f}" for s in scores],
                textposition="top center",
                textfont={"size": 12},
                marker={"size": 10, "color": marker_colors},
                line={"color": "#4fc3f7", "width": 2},
            )
        )

        fig.update_layout(
            xaxis={
                "categoryorder": "array",
                "categoryarray": x_labels,
            },
            yaxis={"range": [0, 100], "dtick": 20, "showline": True},
            showlegend=False,
            height=300,
        )
        return fig

    def fig_HeatmapGroceryCat(self, pdf: pd.DataFrame) -> go.Figure:
        pdf_monthly = pdf[pdf["Freq"] == "Monthly"].copy()
        sorted_raw = sorted(pdf_monthly["Period"].unique().tolist())
        x_labels = [pd.to_datetime(p).strftime("%b %Y") for p in sorted_raw]
        label_map = dict(zip(sorted_raw, x_labels, strict=True))
        pdf_monthly["Period"] = pdf_monthly["Period"].map(label_map)

        cat_order = (
            pdf_monthly.groupby("category_main")["total_CHF"]
            .sum()
            .sort_values(ascending=False)
            .index.tolist()
        )
        cat_order = [c for c in cat_order if c != "Other"] + (
            ["Other"] if "Other" in cat_order else []
        )

        pivot = pdf_monthly.pivot_table(
            index="category_main", columns="Period", values="total_CHF", fill_value=0
        )
        pivot = pivot.reindex(index=cat_order, columns=x_labels, fill_value=0)

        text: list[list[str]] = [
            [
                f"{pivot.iloc[r, c]:.0f}" if pivot.iloc[r, c] > 0 else ""  # type: ignore[operator]
                for c in range(pivot.shape[1])
            ]
            for r in range(pivot.shape[0])
        ]

        fig = go.Figure(
            go.Heatmap(
                z=pivot.values,
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale="Greens",
                colorbar={"title": "CHF"},
                hovertemplate="Category: %{y}<br>Month: %{x}<br>CHF: %{z:.2f}<extra></extra>",
                text=text,
                texttemplate="%{text}",
                textfont={"size": 10},
            )
        )

        height = max(220, len(cat_order) * 35 + 80)
        fig.update_layout(
            height=height,
            xaxis={"domain": [0.15, 1.0]},
            yaxis={"autorange": "reversed", "showticklabels": False},
            margin={"l": 10},
        )

        for cat in cat_order:
            fig.add_annotation(
                x=0.0,
                y=cat,
                xref="paper",
                yref="y",
                xanchor="left",
                yanchor="middle",
                text=cat,
                showarrow=False,
            )

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
