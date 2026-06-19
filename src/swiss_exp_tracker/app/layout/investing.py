from __future__ import annotations

from typing import Any

from dash import dcc
from dash import html

from swiss_exp_tracker.app.components.cards import make_figure_card
from swiss_exp_tracker.app.components.cards import make_number_card
from swiss_exp_tracker.app.dash_components import GRAPH_CONFIG
from swiss_exp_tracker.app.dash_components import make_card_title
from swiss_exp_tracker.app.dash_components import make_page_title
from swiss_exp_tracker.app.vis.figure_investing import fig_allocation_donut
from swiss_exp_tracker.app.vis.figure_investing import fig_portfolio_progression


def layout(data: Any, pos: Any) -> Any:
    return html.Div(
        [
            make_page_title("📈 Investing"),
            html.Div(
                [
                    # ── Row 1: KPI cards ──────────────────────────────────────
                    make_number_card("Total Invested", pos.total_invested, width=3),
                    make_number_card("Total Value", pos.total_value, width=3),
                    make_number_card("Unrealised P&L", pos.total_pnl_chf, width=3),
                    make_number_card("P&L %", pos.total_pnl_pct, width=3, unit="%"),
                    # ── Row 2: Performers card | Allocation donut ──────────────
                    html.Div(
                        [
                            # Card header with title + year scope toggle
                            html.Div(
                                [
                                    make_card_title("Performers"),
                                    dcc.RadioItems(
                                        id="investing-year-scope",
                                        options=[  # type: ignore[arg-type]
                                            {
                                                "label": "  Current year",
                                                "value": "year",
                                            },
                                            {"label": "  All time", "value": "all"},
                                        ],
                                        value="year",
                                        inline=True,
                                        labelStyle={"marginRight": "1rem"},
                                    ),
                                ],
                                className="card-header-with-buttons",
                            ),
                            # Dynamic content: best + worst stacked
                            html.Div(id="investing-performers-content"),
                        ],
                        className="card card-kpi col-6",
                    ),
                    make_figure_card(
                        "Allocation",
                        fig_allocation_donut(pos.pdf_latest),
                        width=6,
                    ),
                    # ── Row 3: Portfolio progression ──────────────────────────
                    make_figure_card(
                        "Portfolio Progression",
                        fig_portfolio_progression(pos.pdf),
                        width=12,
                    ),
                    # ── Row 4: Individual position charts ─────────────────────
                    html.Div(
                        [
                            make_card_title("Position Progression"),
                            dcc.Dropdown(
                                id="investing-symbol-dropdown",
                                options=[  # type: ignore[arg-type]
                                    {
                                        "label": f"{s}  —  {pos.symbol_to_name.get(s, s)}",
                                        "value": s,
                                    }
                                    for s in pos.all_symbols
                                ],
                                value=pos.all_symbols,
                                multi=True,
                                clearable=False,
                                className="dropdown-spaced",
                            ),
                            make_card_title("Value (CHF)"),
                            dcc.Graph(
                                id="investing-pos-value",
                                className="pos-graph",
                                config=GRAPH_CONFIG,
                            ),
                            make_card_title("P&L (%)"),
                            dcc.Graph(
                                id="investing-pos-pct",
                                className="pos-graph",
                                config=GRAPH_CONFIG,
                            ),
                        ],
                        className="card card-graph col-12",
                    ),
                ],
                className="grid",
            ),
        ]
    )
