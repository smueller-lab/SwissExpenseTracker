from __future__ import annotations

from typing import Any

from dash import html

from swiss_exp_tracker.app.components.cards import make_figure_card
from swiss_exp_tracker.app.components.cards import make_figure_card_MonthYear
from swiss_exp_tracker.app.components.cards import make_number_card
from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.dash_components import make_page_title
from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()
cfg = config()


def layout(data: Any) -> Any:
    """Return the Sport analytics page layout with KPI cards and activity chart."""
    pct_value = (
        data.v_SportPctVsLastYtd if data.v_SportPctVsLastYtd is not None else 0.0
    )
    return html.Div(
        [
            make_page_title("⚽ Sport Analytics"),
            html.Div(
                [
                    make_number_card(
                        "Avg / Year", data.v_SportAvgPerYear, width=4, fmt=",.0f"
                    ),
                    make_number_card(
                        data.s_SportYtdLabel,
                        data.v_SportCurrentYtd,
                        width=4,
                        fmt=",.0f",
                    ),
                    make_number_card(
                        "vs Last Year (YTD)",
                        pct_value,
                        width=4,
                        unit="%",
                        fmt="+.1f",
                    ),
                    make_figure_card_MonthYear(
                        "Sport expenses [CHF]",
                        "fig-Sport",
                        width=12,
                        height=cfg.height_Sport,
                    ),
                    make_figure_card(
                        "Activities per Year",
                        F.fig_BarSportActivities(data.pdf_SportActivities),
                        width=12,
                    ),
                ],
                className="grid",
            ),
        ]
    )
