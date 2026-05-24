from __future__ import annotations

from typing import Any

from dash import html

from swiss_exp_tracker.app.components.cards import make_figure_card
from swiss_exp_tracker.app.components.cards import make_figure_card_MonthYear
from swiss_exp_tracker.app.dash_components import make_page_title
from swiss_exp_tracker.app.vis.figure import Fig


F = Fig()


def layout(data: Any) -> Any:
    return html.Div(
        [
            make_page_title("🍽️ Dining Analytics"),
            html.Div(
                [
                    make_figure_card_MonthYear(
                        "Food & Dining expenses [CHF]", "fig-Food", width=12
                    ),
                    make_figure_card(
                        "Food & Dining expenses per visit [CHF]",
                        F.fig_BoxFood(data.pdf_Master),
                        width=12,
                    ),
                ],
                className="grid",
            ),
        ]
    )
