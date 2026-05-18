from __future__ import annotations

from typing import Any

from dash import html

from app.components.cards import make_double_figure_card_MonthYear
from app.components.cards import make_figure_card
from app.dash_components import make_page_title
from app.vis.figure import Fig


F = Fig()


def layout(data: Any) -> Any:
    return html.Div(
        [
            make_page_title("🛒 Grocery store Analytics"),
            html.Div(
                [
                    make_double_figure_card_MonthYear(
                        title_abs="Grocery store expenses [CHF]",
                        fig_id_abs="fig-Abs",
                        title_pct="Grocery store expenses [%]",
                        fig_id_pct="fig-Pct",
                        width=12,
                    ),
                    make_figure_card(
                        "Grocery store expenses per visit [CHF]",
                        F.fig_BoxGrocery(data.pdf_GroceryVisits),
                        width=12,
                    ),
                ],
                className="grid",
            ),
        ]
    )
