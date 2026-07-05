from __future__ import annotations

from typing import Any

import pandas as pd

from dash import html

from swiss_exp_tracker.app.components.cards import make_figure_card
from swiss_exp_tracker.app.components.cards import make_number_card
from swiss_exp_tracker.app.dash_components import make_page_title
from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()


def layout(data: Any) -> Any:
    """Return the vacation analytics page with KPI row, bar chart, boxplot, and donut."""
    pdf: pd.DataFrame = data.pdf_Vacation

    max_year = pdf["Year"].max()
    current_year_total = float(pdf.loc[pdf["Year"] == max_year, "Total"].sum())

    n_years = int(pdf["Year"].nunique())
    avg_per_year = float(pdf["Total"].sum() / n_years) if n_years > 0 else 0.0

    top_category: str = (
        str(pdf.groupby("category_second")["Total"].sum().idxmax())
        if not pdf.empty
        else "N/A"
    )

    totals = (
        pdf.groupby("category_second")["Total"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    return html.Div(
        [
            make_page_title("🏖️ Vacation Analytics"),
            html.Div(
                [
                    # Row 1 — KPIs (3x col-4 = 12)
                    make_number_card(
                        "Current Year",
                        current_year_total,
                        width=4,
                        fmt=",.0f",
                    ),
                    make_number_card(
                        "Avg / Year",
                        avg_per_year,
                        width=4,
                        fmt=",.0f",
                    ),
                    make_number_card(
                        "Top Category",
                        top_category,
                        width=4,
                        fmt="",
                        unit="",
                        value_class="kpi-value",
                    ),
                    # Row 2 — Full-width bar (col-12)
                    make_figure_card(
                        "Vacation expenses [CHF]",
                        F.fig_BarVacation(pdf),
                        width=12,
                    ),
                    # Row 3 — Boxplot + Donut (col-7 + col-5 = 12)
                    make_figure_card(
                        "Spend per transaction — top 4 categories [CHF]",
                        F.fig_BoxplotVacation(data.pdf_VacationTransactions),
                        width=7,
                    ),
                    make_figure_card(
                        "By Category",
                        F.fig_DonutByCategory(
                            totals,
                            col_category="category_second",
                            col_amount="Total",
                        ),
                        width=5,
                    ),
                ],
                className="grid",
            ),
        ]
    )
