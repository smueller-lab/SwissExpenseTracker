from __future__ import annotations

from typing import Any

from dash import html

from app.components.cards import make_figure_card
from app.dash_components import make_page_title
from app.vis.figure import Fig


F = Fig()


def layout(data: Any) -> Any:
    return html.Div(
        [
            make_page_title("🏖️ Vacation Analytics"),
            html.Div(
                [
                    make_figure_card(
                        "Vacation expenses [CHF]",
                        F.fig_BarVacation(data.pdf_Vacation),
                        width=12,
                    )
                ],
                className="grid",
            ),
        ]
    )
