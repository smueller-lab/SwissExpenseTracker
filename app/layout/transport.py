from __future__ import annotations

from typing import Any

from dash import html

from app.components.cards import make_figure_card
from app.config import config
from app.dash_components import make_page_title
from app.vis.figure import Fig


F = Fig()
cfg = config()


def layout(data: Any) -> Any:
    return html.Div(
        [
            make_page_title("🚄 Transport Analytics"),
            html.Div(
                [
                    make_figure_card(
                        "Transport expenses [CHF]",
                        F.fig_BarYearlyByCategory(
                            pdf=data.pdf_Transport,
                            col_catgeory="category_transport",
                            col_amount="amount_CHF",
                            dTick=cfg.dTick_Transport,
                            npixel=cfg.npixel_Transport,
                        ),
                        width=12,
                    ),
                    make_figure_card(
                        "Transport Heatmap per month [CHF]",
                        F.fig_HeatmapMonthly(data.pdf_TransportHeatmap),
                        width=12,
                    ),
                ],
                className="grid",
            ),
        ]
    )
