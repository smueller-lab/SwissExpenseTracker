from __future__ import annotations

from typing import Any

from dash import dcc
from dash import html

from swiss_exp_tracker.app.components.cards import make_figure_card
from swiss_exp_tracker.app.components.cards import make_figure_card_MonthYear
from swiss_exp_tracker.app.components.cards import make_number_card
from swiss_exp_tracker.app.components.cards import make_table_card
from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.dash_components import GRAPH_CONFIG
from swiss_exp_tracker.app.dash_components import make_card_title
from swiss_exp_tracker.app.libs import get_heightFigure
from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()
cfg = config()

_TOP_ARTICLES_COLS = [
    {"name": "Article", "id": "article"},
    {"name": "Category", "id": "category_main"},
    {"name": "Count", "id": "count", "header_class": ""},
    {"name": "Total CHF", "id": "total_chf"},
]


def _health_class(score: float) -> str:
    if score >= 70:
        return "kpi-value kpi-positive"
    if score >= 40:
        return "kpi-value kpi-warning"
    return "kpi-value kpi-negative"


def layout(data: Any) -> Any:
    score = data.n_HealthScore_latest

    top_art = data.pdf_GroceryTopArticles.head(15).copy()
    top_art["count"] = top_art["count"].astype(int).astype(str)

    return html.Div(
        [
            html.Div(
                [
                    html.H2(
                        [
                            html.Span("M", className="migros-m"),
                            " Cumulus Analytics",
                        ],
                        className="page-title-center",
                    )
                ],
                className="page-title-wrap",
            ),
            dcc.Store(id="gd-store-donut", data=None),
            html.Div(
                [
                    # ── KPI row ───────────────────────────────────────────
                    make_number_card(
                        "Monthly Spend",
                        data.n_GroceryMonthlySpend,
                        width=3,
                    ),
                    make_number_card(
                        "Avg. per Visit",
                        data.n_GroceryAvgPerVisit,
                        width=3,
                    ),
                    make_number_card(
                        "Visits this Month",
                        float(data.n_GroceryVisitsMonth),
                        width=3,
                        unit="",
                        fmt=".0f",
                    ),
                    make_number_card(
                        "Health Score",
                        score,
                        width=3,
                        unit="/ 100",
                        value_class=_health_class(score),
                    ),
                    # ── Category trend (full width) ───────────────────────
                    make_figure_card_MonthYear(
                        "Category Spend Over Time [CHF]",
                        fig_id="gd-fig-cat",
                        width=12,
                        height=get_heightFigure(
                            cfg.vk_npixel_GroceryCat["Monthly"], F.vk_Margin
                        ),
                    ),
                    # ── Heatmap (full width) ──────────────────────────────
                    make_figure_card(
                        "Spend Heatmap — Category x Month [CHF]",
                        F.fig_HeatmapGroceryCat(data.pdf_GroceryCat),
                        width=12,
                    ),
                    # ── Health index (full width) ─────────────────────────
                    make_figure_card(
                        "Healthy Grocery Index  (0 = poor · 100 = optimal)",
                        F.fig_HealthIndex(data.pdf_GroceryHealth),
                        width=12,
                    ),
                    # ── Donut + Table ─────────────────────────────────────
                    html.Div(
                        [
                            html.Div(
                                [
                                    make_card_title("Category Distribution"),
                                    html.Button(
                                        "← Back",
                                        id="gd-btn-back",
                                        n_clicks=0,
                                        className="btn-toggle is-hidden",
                                    ),
                                ],
                                className="card-header-with-buttons",
                            ),
                            dcc.Graph(
                                id="gd-fig-donut",
                                figure=F.fig_DonutGroceryCat(
                                    data.pdf_GroceryItems, None
                                ),
                                config=GRAPH_CONFIG,
                            ),
                        ],
                        className="card card-graph col-6",
                    ),
                    make_table_card(
                        "Top Bought Articles",
                        _TOP_ARTICLES_COLS,
                        top_art.to_dict("records"),
                        width=6,
                    ),
                ],
                className="grid",
            ),
        ]
    )
