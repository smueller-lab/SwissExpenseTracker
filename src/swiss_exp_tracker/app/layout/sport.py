from __future__ import annotations

from typing import Any

from dash import html

from swiss_exp_tracker.app.components.cards import make_figure_card_MonthYear
from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.dash_components import make_page_title
from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()
cfg = config()


def layout(data: Any) -> Any:
    return html.Div(
        [
            make_page_title("⛳ Sport Analytics"),
            html.Div(
                [
                    make_figure_card_MonthYear(
                        "Sport expenses [CHF]", "fig-Sport", width=12
                    )
                ],
                className="grid",
            ),
        ]
    )
