from __future__ import annotations

from typing import Any
from typing import Literal

import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]
import plotly.io as pio  # pyright: ignore[reportMissingTypeStubs]

import swiss_exp_tracker.app.vis.ploty_template  # pyright: ignore[reportUnusedImport] # noqa: F401  # registers myTemp Plotly template

from swiss_exp_tracker.app.config import VIS
from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.libs import get_adaptive_dTick
from swiss_exp_tracker.app.libs import get_heightFigure
from swiss_exp_tracker.app.libs import get_rxAxis_Date
from swiss_exp_tracker.app.libs import get_ryAxis

vis = VIS()
cfg = config()
pio.templates.default = "myTemp"


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Convert a #RRGGBB hex string to an rgba() string with the given alpha."""
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i : i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r}, {g}, {b}, {alpha})"


def _build_category_colors(
    categories: list[str], col_map: dict[str, str]
) -> dict[str, str]:
    """Map each category to its named colour, else a distinct cycling-palette colour."""
    used = set(col_map.values())
    palette = [c for c in vis.vk_cycling_col if c not in used] or vis.vk_cycling_col
    colors: dict[str, str] = {}
    fallback_idx = 0
    for category in categories:
        if category in col_map:
            colors[category] = col_map[category]
        else:
            colors[category] = palette[fallback_idx % len(palette)]
            fallback_idx += 1
    return colors


def _collapse_top_n(
    pdf: pd.DataFrame,
    col_category: str,
    col_amount: str,
    n: int = cfg.category_top_n,
) -> pd.DataFrame:
    """Relabel rows whose category is outside the top-n (by total amount) as 'Other'."""
    totals = pdf.groupby(col_category)[col_amount].sum().sort_values(ascending=False)
    if len(totals) <= n:
        return pdf
    keep = set(totals.head(n).index)
    out = pdf.copy()
    out[col_category] = out[col_category].where(
        out[col_category].isin(keep), cfg.category_other_label
    )
    return out


def _order_with_other_last(categories: list[str]) -> list[str]:
    """Return categories with the 'Other' bucket moved to the end, order otherwise kept."""
    other = cfg.category_other_label
    if other in categories:
        return [c for c in categories if c != other] + [other]
    return categories


def _aggregate_top_n(
    pdf: pd.DataFrame,
    col_category: str,
    col_amount: str,
    n: int = cfg.category_top_n,
) -> pd.DataFrame:
    """Aggregate to one row per top-n category plus a trailing 'Other' bucket, sorted desc."""
    grouped = (
        _collapse_top_n(pdf, col_category, col_amount, n)
        .groupby(col_category, as_index=False)[col_amount]
        .sum()
        .sort_values(col_amount, ascending=False)
        .reset_index(drop=True)
    )
    other = cfg.category_other_label
    if other in grouped[col_category].values:
        is_other = grouped[col_category] == other
        grouped = pd.concat([grouped[~is_other], grouped[is_other]], ignore_index=True)
    return grouped


def _make_no_data_fig(label: str) -> go.Figure:
    """Return a styled placeholder figure when data is unavailable."""
    return go.Figure(
        layout=go.Layout(
            annotations=[
                go.layout.Annotation(
                    text=label,
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=cfg.font_no_data,
                )
            ],
            xaxis={"visible": False},
            yaxis={"visible": False},
        )
    )


class Fig:
    def __init__(self) -> None:
        self.vk_Margin = pio.templates[
            "myTemp"
        ].layout.margin  # pyright: ignore[reportOptionalMemberAccess, reportAttributeAccessIssue]

    def fig_IncomeExpenseMonthly(self, pdf: pd.DataFrame) -> go.Figure:
        """Return a dual line+area chart of monthly income vs expenses; plots only the rows given."""
        if pdf.empty:
            return _make_no_data_fig("income vs expenses")

        pdf_sorted = pdf.sort_values("Month").reset_index(drop=True)
        x_dates = pd.to_datetime(pdf_sorted["Month"])

        expense_color = vis.vk_IncomeExpense_col["Expenses"]
        income_color = vis.vk_IncomeExpense_col["Income"]

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=x_dates,
                y=pdf_sorted["Expense"],
                mode="lines+markers",
                name="Expenses",
                line={"color": expense_color, "width": 2},
                marker={"color": expense_color, "size": 8},
                fill="tozeroy",
                fillcolor=_hex_to_rgba(expense_color, 0.15),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x_dates,
                y=pdf_sorted["Income"],
                mode="lines+markers",
                name="Income",
                line={"color": income_color, "width": 2},
                marker={"color": income_color, "size": 8},
                fill="tozeroy",
                fillcolor=_hex_to_rgba(income_color, 0.15),
            )
        )

        combined = pd.concat([pdf["Expense"], pdf["Income"]])
        dTick = get_adaptive_dTick(float(combined.max()))
        ry_Axis = get_ryAxis(dTick, combined, True)
        # Wider top margin than the template default (t=20) to fit the
        # above-plot legend below without crowding it — folded into the
        # height calc so the plot area keeps its usual size instead of
        # shrinking by the extra margin.
        margin_TopLegend = {"t": 60, "b": self.vk_Margin["b"]}
        height_Figure = get_heightFigure(cfg.npixel_IncomeExpense, margin_TopLegend)
        s_tick_val, s_tick_text, format_Date = get_rxAxis_Date(x_dates)

        x_min, x_max = x_dates.min(), x_dates.max()
        if x_min == x_max:
            pad = pd.Timedelta(days=15)
            x_range = [x_min - pad, x_max + pad]
        else:
            x_range = [x_min, x_max]

        fig.update_layout(
            xaxis={
                "tickvals": s_tick_val,
                "ticktext": s_tick_text,
                "tickformat": format_Date,
                "range": x_range,
                "showline": True,
                "linecolor": "white",
            },
            yaxis={
                "dtick": dTick,
                "range": ry_Axis,
                "showline": True,
                "linecolor": "white",
            },
            margin={"t": margin_TopLegend["t"]},
            height=height_Figure,
            showlegend=True,
            # One-off exception to the site-wide right-side legend (see
            # plots.md): this chart only has two series (Income/Expenses),
            # so a horizontal legend above the plot reads better than a
            # tall empty right gutter. legendFixed in meta tells
            # mobile_legend.js to leave this position alone at every
            # breakpoint instead of applying the usual responsive rules.
            legend={
                "orientation": "h",
                "yanchor": "bottom",
                "y": 1.02,
                "xanchor": "center",
                "x": 0.5,
            },
            meta={"legendFixed": True},
        )

        return fig

    def fig_BarGrocery(
        self, pdf_Grocery: pd.DataFrame, Freq: Literal["Monthly", "Yearly"]
    ) -> go.Figure:
        """Return a stacked bar chart of grocery spending per merchant."""
        if pdf_Grocery.empty:
            return _make_no_data_fig("grocery spending")
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

        dTick_Grocery = get_adaptive_dTick(float(pdf_Grocery["totalPeriod_CHF"].max()))
        npixel_Grocery = cfg.vk_npixel_Grocery[Freq]
        ry_Axis = get_ryAxis(dTick_Grocery, pdf_Grocery["totalPeriod_CHF"], True)

        height_Figure = get_heightFigure(npixel_Grocery, self.vk_Margin)

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": dTick_Grocery, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_BarGrocery_pct(
        self, pdf_Grocery: pd.DataFrame, Freq: Literal["Monthly", "Yearly"]
    ) -> go.Figure:
        """Return a stacked bar chart of grocery merchant share as percentages."""
        if pdf_Grocery.empty:
            return _make_no_data_fig("grocery share")
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

        height_Figure = get_heightFigure(cfg.npixel_Pct, self.vk_Margin)

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": cfg.dTick_Pct, "range": cfg.ry_Axis_Pct, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_BoxGrocery(self, pdf: pd.DataFrame) -> go.Figure:
        """Return a box plot of per-visit spend for each grocery merchant."""
        if pdf.empty:
            return _make_no_data_fig("grocery visits")
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

            stick_Text.append(f"{Merchant}<br>(n={len(pdf_Merchant)})")
            stick_Val.append(i)

        pdf_Merchant = pdf[pdf["Merchant"].isin(vis.s_Merchant_Grocery)].reset_index(
            drop=True
        )
        npixel_Grocery = cfg.vk_npixel_Grocery["Visit"]
        dTick_Grocery = get_adaptive_dTick(float(pdf_Merchant["amount_CHF"].max()))
        ry_Axis = get_ryAxis(dTick_Grocery, pdf_Merchant["amount_CHF"], True)
        height_Figure = get_heightFigure(npixel_Grocery, self.vk_Margin)

        fig.update_layout(
            xaxis={"tickmode": "array", "tickvals": stick_Val, "ticktext": stick_Text},
            yaxis={"dtick": dTick_Grocery, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_BarFood(
        self, pdf_Food: pd.DataFrame, Freq: Literal["Monthly", "Yearly"]
    ) -> go.Figure:
        """Return a stacked bar chart of food spending by category."""
        if pdf_Food.empty:
            return _make_no_data_fig("food spending")
        if Freq not in ["Monthly", "Yearly"]:
            raise ValueError(f"Invalid Freq={Freq}, Expected one of: Monthly, Yearly")

        fig = go.Figure()

        pdf_Food = pdf_Food[pdf_Food["Freq"] == Freq].reset_index(drop=True)
        pdf_Food = _collapse_top_n(
            pdf_Food, "category_second", "total_CHF", n=cfg.category_top_n_bar
        )
        s_Category_sort = _order_with_other_last(
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
                    marker={"color": vis.vk_Food_col.get(Category, vis.fallback_col)},
                )
            )

        dTick_Food = get_adaptive_dTick(float(pdf_Food["totalPeriod_CHF"].max()))
        ry_Axis = get_ryAxis(dTick_Food, pdf_Food["totalPeriod_CHF"], True)

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": dTick_Food, "range": ry_Axis, "showline": True},
            height=cfg.height_Food,
        )

        return fig

    def fig_BoxFood(self, pdf: pd.DataFrame) -> go.Figure:
        """Return a box plot of per-transaction spend for each food category."""
        if pdf.empty:
            return _make_no_data_fig("food categories")
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
                    marker={"color": vis.vk_Food_col.get(Category, vis.fallback_col)},
                )
            )

            stick_Text.append(f"{Category}<br>(n={len(pdf_Category)})")
            stick_Val.append(i)

        pdf_Category = pdf[
            pdf["category_second"].isin(vis.s_Category_Food)
        ].reset_index(drop=True)
        npixel_Food = cfg.vk_npixel_Food["Visit"]
        dTick_Food = get_adaptive_dTick(float(pdf_Category["amount_CHF"].max()))
        ry_Axis = get_ryAxis(dTick_Food, pdf_Category["amount_CHF"], True)
        height_Figure = get_heightFigure(npixel_Food, self.vk_Margin)

        fig.update_layout(
            xaxis={"tickmode": "array", "tickvals": stick_Val, "ticktext": stick_Text},
            yaxis={"dtick": dTick_Food, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_DonutCategoryMain(self, pdf_CatMain: pd.DataFrame) -> go.Figure:
        """Return a donut chart of total spend broken down by main expense category."""
        if pdf_CatMain.empty:
            return _make_no_data_fig("expense categories")
        pdf_CatMain = _aggregate_top_n(pdf_CatMain, "category_main", "amount_CHF")
        fig = go.Figure(
            go.Pie(
                labels=pdf_CatMain["category_main"],
                values=pdf_CatMain["amount_CHF"],
                hole=cfg.donut_hole,
                textinfo="percent+label",
                textfont=cfg.textfont_label,
                pull=[cfg.donut_pull] * len(pdf_CatMain),
                domain=cfg.donut_domain,
            )
        )

        fig.update_layout(
            showlegend=False,
            height=get_heightFigure(cfg.npixel_Donut, self.vk_Margin),
        )

        return fig

    def fig_DonutByCategory(
        self,
        pdf: pd.DataFrame,
        col_category: str,
        col_amount: str,
        min_pct: float = cfg.donut_min_pct,
        col_map: dict[str, str] | None = None,
    ) -> go.Figure:
        """Return a donut for a category/amount pair; folds the tail into 'Other', hides labels below min_pct."""
        if pdf.empty:
            return _make_no_data_fig("category breakdown")
        pdf = _aggregate_top_n(pdf, col_category, col_amount)
        labels = pdf[col_category].tolist()
        values = pdf[col_amount]
        total = values.sum()
        pct = values / total * 100 if total > 0 else values * 0

        text = [
            f"{label}<br>{p:.1f}%" if p >= min_pct else ""
            for label, p in zip(labels, pct, strict=True)
        ]

        marker = None
        if col_map is not None:
            colors = _build_category_colors(labels, col_map)
            colors[cfg.category_other_label] = vis.fallback_col
            marker = {"colors": [colors[label] for label in labels]}

        fig = go.Figure(
            go.Pie(
                labels=labels,
                values=values,
                hole=cfg.donut_hole,
                text=text,
                textinfo="text",
                textfont=cfg.textfont_label,
                pull=[cfg.donut_pull] * len(labels),
                domain=cfg.donut_domain,
                hovertemplate=cfg.donut_hovertemplate,
                marker=marker,
            )
        )
        fig.update_layout(
            showlegend=False,
            height=get_heightFigure(cfg.npixel_Donut, self.vk_Margin),
        )
        return fig

    def fig_BarVacation(self, pdf_Vacation: pd.DataFrame) -> go.Figure:
        """Return a stacked bar chart of vacation spending by year and category."""
        if pdf_Vacation.empty:
            return _make_no_data_fig("vacation spending")
        fig = go.Figure()

        pdf_Vacation = _collapse_top_n(
            pdf_Vacation, "category_second", "Total", n=cfg.category_top_n_bar
        )
        z_YearExpense = pdf_Vacation.groupby("Year")["Total"].sum()
        category_order = _order_with_other_last(
            pdf_Vacation.groupby("category_second")["Total"]
            .sum()
            .sort_values(ascending=False)
            .index.tolist()
        )

        for Category in category_order:
            group = pdf_Vacation[pdf_Vacation["category_second"] == Category]
            bar_kwargs: dict[str, object] = {
                "x": group["Year"],
                "y": group["Total"],
                "name": Category,
            }
            if Category == cfg.category_other_label:
                bar_kwargs["marker"] = {"color": vis.fallback_col}
            fig.add_trace(go.Bar(**bar_kwargs))

        dTick_Vacation = get_adaptive_dTick(float(z_YearExpense.max()))
        ry_Axis = get_ryAxis(dTick_Vacation, z_YearExpense, True)
        height_Figure = get_heightFigure(cfg.npixel_Vacation, self.vk_Margin)

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": dTick_Vacation, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_BoxplotVacation(self, pdf: pd.DataFrame) -> go.Figure:
        """Return a boxplot of per-transaction spend for the top 4 vacation categories."""
        if pdf.empty:
            return _make_no_data_fig("vacation transaction data")

        totals = pdf.groupby("category_second")["amount"].sum()
        top4 = (
            totals.nlargest(cfg.vacation_top_n)
            .sort_values(ascending=False)
            .index.tolist()
        )
        pdf_top4 = pdf[pdf["category_second"].isin(top4)].copy()

        dTick = get_adaptive_dTick(float(pdf_top4["amount"].max()))
        ry_Axis = get_ryAxis(dTick, pdf_top4["amount"], True)

        fig = go.Figure()
        for category in top4:
            fig.add_trace(
                go.Box(
                    y=pdf_top4[pdf_top4["category_second"] == category]["amount"],
                    name=category,
                    boxpoints="outliers",
                )
            )

        fig.update_layout(
            showlegend=False,
            yaxis={"dtick": dTick, "range": ry_Axis, "showline": True},
        )
        return fig

    def fig_BarYearlyByCategory(
        self,
        pdf: pd.DataFrame,
        col_catgeory: str,
        col_amount: str,
        npixel: float,
        col_map: dict[str, str] | None = None,
    ) -> go.Figure:
        """Return a stacked bar chart of yearly totals grouped by a category column."""
        if pdf.empty:
            return _make_no_data_fig("yearly category spending")
        fig = go.Figure()

        pdf = _collapse_top_n(pdf, col_catgeory, col_amount, n=cfg.category_top_n_bar)
        z_YearExpense = pdf.groupby("Year")[col_amount].sum()

        category_order = _order_with_other_last(
            pdf.groupby(col_catgeory)[col_amount]
            .sum()
            .sort_values(ascending=False)
            .index.tolist()
        )

        for Category in category_order:
            group = pdf[pdf[col_catgeory] == Category]
            bar_kwargs: dict[str, object] = {
                "x": group["Year"],
                "y": group[col_amount],
                "name": Category,
            }
            if Category == cfg.category_other_label:
                bar_kwargs["marker"] = {"color": vis.fallback_col}
            elif col_map is not None:
                bar_kwargs["marker"] = {
                    "color": col_map.get(Category, vis.fallback_col)
                }
            fig.add_trace(go.Bar(**bar_kwargs))

        dTick = get_adaptive_dTick(float(z_YearExpense.max()))
        ry_Axis = get_ryAxis(dTick, z_YearExpense, True)
        height_Figure = get_heightFigure(npixel, self.vk_Margin)

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
        npixel: float,
        col_map: dict[str, str] | None = None,
    ) -> go.Figure:
        """Return a stacked bar chart of spend per period, grouped by a category column."""
        if pdf.empty:
            return _make_no_data_fig("category spending")
        fig = go.Figure()

        if col_map is None:
            col_map = vis.vk_Sport_col

        pdf_Freq = pdf[pdf["Freq"] == Freq].reset_index(drop=True)
        pdf_Freq = _collapse_top_n(
            pdf_Freq, col_catgeory, col_amount, n=cfg.category_top_n_bar
        )

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
        # keep "Other" at the top of the stack (drawn last) and always grey
        s_category_orderstack_order = _order_with_other_last(
            z_category_totals.index.tolist()
        )
        color_map = _build_category_colors(s_category_orderstack_order, col_map)
        color_map[cfg.category_other_label] = vis.fallback_col

        for Category in s_category_orderstack_order:
            group = pdf_Freq[pdf_Freq[col_catgeory] == Category]
            fig.add_trace(
                go.Bar(
                    x=group["Period"],
                    y=group[col_amount],
                    name=Category,
                    marker={"color": color_map[Category]},
                )
            )

        dTick = get_adaptive_dTick(float(z_FreqExpense.max()))
        ry_Axis = get_ryAxis(dTick, z_FreqExpense, True)
        height_Figure = get_heightFigure(npixel, self.vk_Margin)

        fig.update_layout(
            barmode="stack",
            yaxis={"dtick": dTick, "range": ry_Axis, "showline": True},
            height=height_Figure,
        )

        return fig

    def fig_HeatmapMonthly(self, pdf: pd.DataFrame) -> go.Figure:
        """Return a month x year heatmap of total CHF spend, newest year at top."""
        if pdf.empty:
            return _make_no_data_fig("monthly spending heatmap")
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
                colorscale=vis.vk_heatmap_colorscale["financial"],
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
        """Return a stacked bar chart of grocery spend by main category."""
        if pdf.empty:
            return _make_no_data_fig(
                "No Migros Cumulus data found. Import your Cumulus receipt data first."
            )
        if Freq not in ["Monthly", "Yearly"]:
            raise ValueError(f"Invalid Freq={Freq}, Expected one of: Monthly, Yearly")

        fig = go.Figure()
        pdf_freq = pdf[pdf["Freq"] == Freq].reset_index(drop=True)
        pdf_freq = _collapse_top_n(
            pdf_freq, "category_main", "total_CHF", n=cfg.category_top_n_bar
        )

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

        cat_order = _order_with_other_last(
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
                    marker={"color": vis.vk_GroceryCat_col.get(cat, vis.fallback_col)},
                )
            )

        npixel = cfg.vk_npixel_GroceryCat[Freq]
        total_per_period = pdf_freq.groupby("Period")["total_CHF"].sum()
        dTick = get_adaptive_dTick(float(total_per_period.max()))
        ry_Axis = get_ryAxis(dTick, total_per_period, True)
        height_Figure = get_heightFigure(npixel, self.vk_Margin)

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
                f"#{round(r * t + 255 * (1 - t)):02x}"
                f"{round(g * t + 255 * (1 - t)):02x}"
                f"{round(b * t + 255 * (1 - t)):02x}"
            )
        return shades

    def fig_DonutGroceryCat(
        self,
        pdf_items: pd.DataFrame,
        category_main: str | None = None,
        category_detail: str | None = None,
    ) -> go.Figure:
        """Return a donut chart of grocery item spend, drillable by main/detail category."""
        if pdf_items.empty:
            return _make_no_data_fig(
                "No Migros Cumulus data found. Import your Cumulus receipt data first."
            )
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
            colors = [vis.vk_GroceryCat_col.get(c, vis.fallback_col) for c in labels]
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
            base = vis.vk_GroceryCat_col.get(category_main, vis.fallback_col)
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
            base = vis.vk_GroceryCat_col.get(category_main, vis.fallback_col)
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
                hole=cfg.donut_hole,
                text=text,
                textinfo="text",
                textfont=cfg.textfont_label_dense,
                pull=[cfg.donut_pull] * len(labels),
                domain=cfg.donut_domain,
                marker={"colors": colors},
                hovertemplate=cfg.donut_hovertemplate,
            )
        )
        fig.update_layout(
            showlegend=False,
            height=get_heightFigure(cfg.npixel_Donut, self.vk_Margin),
        )
        return fig

    def fig_HealthIndex(self, pdf: pd.DataFrame) -> go.Figure:
        """Return a line chart of monthly grocery health index scores with colour-coded bands."""
        if pdf.empty:
            return _make_no_data_fig(
                "No Migros Cumulus data found. Import your Cumulus receipt data first."
            )
        fig = go.Figure()

        fig.add_hrect(
            y0=cfg.health_score_good,
            y1=cfg.health_score_max,
            fillcolor=vis.vk_HealthBand_col["good"],
            opacity=cfg.health_band_opacity,
            line_width=0,
        )
        fig.add_hrect(
            y0=cfg.health_score_ok,
            y1=cfg.health_score_good,
            fillcolor=vis.vk_HealthBand_col["ok"],
            opacity=cfg.health_band_opacity,
            line_width=0,
        )
        fig.add_hrect(
            y0=0,
            y1=cfg.health_score_ok,
            fillcolor=vis.vk_HealthBand_col["bad"],
            opacity=cfg.health_band_opacity,
            line_width=0,
        )

        pdf = pdf.sort_values("Period").reset_index(drop=True)
        sorted_raw = pdf["Period"].tolist()
        x_labels = [pd.to_datetime(p).strftime("%b %Y") for p in sorted_raw]

        scores = [float(s) for s in pdf["score"]]
        marker_colors = [
            (
                vis.vk_HealthBand_col["good"]
                if s >= cfg.health_score_good
                else (
                    vis.vk_HealthBand_col["ok"]
                    if s >= cfg.health_score_ok
                    else vis.vk_HealthBand_col["bad"]
                )
            )
            for s in scores
        ]

        fig.add_trace(
            go.Scatter(
                x=x_labels,
                y=pdf["score"],
                mode="lines+markers+text",
                text=[f"{s:.0f}" for s in scores],
                textposition="top center",
                textfont=cfg.textfont_label,
                marker={"size": 10, "color": marker_colors},
                line={"color": vis.health_line_col, "width": 2},
            )
        )

        fig.update_layout(
            xaxis={
                "categoryorder": "array",
                "categoryarray": x_labels,
            },
            yaxis={
                "range": [0, cfg.health_score_max],
                "dtick": 20,
                "showline": True,
            },
            showlegend=False,
            height=cfg.height_HealthIndex,
        )
        return fig

    def fig_HeatmapGroceryCat(self, pdf: pd.DataFrame) -> go.Figure:
        """Return a monthly heatmap of grocery spend per category with CHF annotations."""
        if pdf.empty:
            return _make_no_data_fig(
                "No Migros Cumulus data found. Import your Cumulus receipt data first."
            )
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

        # Color by each category's share of that month's total, not its raw
        # CHF value — a global CHF-based color scale makes every category in
        # a low-spend month look uniformly "cheap", hiding which categories
        # actually dominated that month. Cell text stays CHF (the concrete
        # number); only the color driver changes. month_totals.replace(0, 1)
        # guards zero-spend months from a division by zero — the numerator
        # is 0 there too, so the result is still 0.
        month_totals = pivot.sum(axis=0)
        pivot_pct = pivot.divide(month_totals.replace(0, 1), axis=1) * 100

        pivot_vals = pivot.to_numpy(dtype=float)
        text: list[list[str]] = [
            [
                f"{pivot_vals[r, c]:.0f}" if pivot_vals[r, c] > 0 else ""
                for c in range(pivot.shape[1])
            ]
            for r in range(pivot.shape[0])
        ]

        fig = go.Figure(
            go.Heatmap(
                z=pivot_pct.values,
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                zmin=0,
                zmax=100,
                colorscale=vis.vk_heatmap_colorscale["category_spend"],
                colorbar={"title": "% of month"},
                hovertemplate=(
                    "Category: %{y}<br>Month: %{x}"
                    "<br>CHF: %{text}<br>Share of month: %{z:.1f}%<extra></extra>"
                ),
                text=text,
                texttemplate="%{text}",
                textfont=cfg.textfont_heatmap,
            )
        )

        height = max(
            cfg.heatmap_min_height,
            len(cat_order) * cfg.heatmap_row_px + cfg.heatmap_height_pad,
        )
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

    def fig_BarSportActivities(self, pdf: pd.DataFrame) -> go.Figure:
        """Return a grouped yearly bar chart of activity counts for Golf, Tennis, and Padel."""
        if pdf.empty:
            return _make_no_data_fig("sport activity data")

        sorted_years = sorted(pdf["year"].unique().tolist())
        year_strs = [str(y) for y in sorted_years]

        sport_order = (
            pdf.groupby("sport")["activity_count"]
            .sum()
            .sort_values(ascending=False)
            .index.tolist()
        )

        fig = go.Figure()
        for sport in sport_order:
            group = pdf[pdf["sport"] == sport].set_index("year")["activity_count"]
            y_vals = [int(group.get(yr, 0)) for yr in sorted_years]
            fig.add_trace(
                go.Bar(
                    x=year_strs,
                    y=y_vals,
                    name=sport,
                    marker={"color": vis.vk_Sport_col.get(sport, vis.fallback_col)},
                )
            )

        z_totals = pdf.groupby("year")["activity_count"].sum()
        max_total = float(z_totals.max())
        dTick = get_adaptive_dTick(max_total, target_steps=8)
        ry_Axis = get_ryAxis(dTick, z_totals, True)

        fig.update_layout(
            barmode="group",
            xaxis={
                "type": "category",
                "categoryorder": "array",
                "categoryarray": year_strs,
            },
            yaxis={"dtick": dTick, "range": ry_Axis, "showline": True},
            height=cfg.height_Sport,
        )

        return fig


def get_fig_BudgetForecast(
    pdf_lines: pd.DataFrame, npixel: int, vk_margin: dict[str, Any]
) -> go.Figure:
    """Return a multi-category cumulative spend forecast figure with solid actual and dotted forecast lines.

    Expects pdf_lines columns: category, period_end (datetime), cumulative_chf, segment ("actual"/"forecast").
    """
    if pdf_lines.empty:
        return _make_no_data_fig("budget forecast")

    # Sort categories by peak cumulative spend descending for cross-chart color consistency.
    category_totals = (
        pdf_lines.groupby("category")["cumulative_chf"]
        .max()
        .sort_values(ascending=False)
    )
    categories = category_totals.index.tolist()
    color_map = _build_category_colors(categories, {})

    fig = go.Figure()

    for category in categories:
        cat_df = pdf_lines[pdf_lines["category"] == category]
        color = color_map[category]

        actual_df = cat_df[cat_df["segment"] == "actual"].sort_values("period_end")
        if not actual_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=actual_df["period_end"],
                    y=actual_df["cumulative_chf"],
                    mode="lines",
                    name=category,
                    line={"color": color, "width": 2},
                    legendgroup=category,
                    showlegend=True,
                )
            )

        forecast_df = cat_df[cat_df["segment"] == "forecast"].sort_values("period_end")
        if not forecast_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=forecast_df["period_end"],
                    y=forecast_df["cumulative_chf"],
                    mode="lines",
                    name=category,
                    line={"color": color, "width": 2, "dash": "dot"},
                    legendgroup=category,
                    showlegend=False,
                )
            )

    dTick = get_adaptive_dTick(float(pdf_lines["cumulative_chf"].max()))
    ry_Axis = get_ryAxis(dTick, pdf_lines["cumulative_chf"], True)
    height_figure = get_heightFigure(npixel, vk_margin)

    x_dates = pd.to_datetime(pdf_lines["period_end"])
    tick_vals, tick_texts, fmt = get_rxAxis_Date(x_dates)
    x_range: list[object] = [x_dates.min() - pd.Timedelta(days=3), tick_vals[-1]]

    fig.update_layout(
        xaxis={
            "tickvals": tick_vals,
            "ticktext": tick_texts,
            "tickformat": fmt,
            "range": x_range,
            "showline": True,
        },
        yaxis={
            "dtick": dTick,
            "range": ry_Axis,
            "showline": True,
        },
        height=height_figure,
        showlegend=True,
        legend={
            "orientation": "v",
            "yanchor": "middle",
            "y": 0.5,
            "xanchor": "left",
            "x": 1.02,
        },
    )

    return fig
