from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]

from dash import dcc
from dash import html

from swiss_exp_tracker.app.config import VIS
from swiss_exp_tracker.app.dash_components import format_diff
from swiss_exp_tracker.app.dash_components import get_balance_class
from swiss_exp_tracker.app.dash_components import make_card_title


vis = VIS()


def make_number_card(
    title: str,
    Number: float,
    width: int = 3,
    unit: str = "CHF",
    fmt: str = ",.2f",
    value_class: str | None = None,
) -> Any:
    suffix = f" {unit}" if unit else ""
    cls = value_class if value_class is not None else get_balance_class(Number)
    return html.Div(
        [
            make_card_title(title),
            html.P(f"{Number:{fmt}}{suffix}", className=cls),
        ],
        className=f"card card-kpi col-{width}",
    )


def make_figure_card(title: str, fig: go.Figure, width: int = 6) -> Any:
    return html.Div(
        [make_card_title(title), dcc.Graph(figure=fig)],
        className=f"card card-graph col-{width}",
    )


def make_figure_card_MonthYear(title: str, fig_id: str, width: int = 6) -> Any:
    return html.Div(
        [
            html.Div(
                [
                    make_card_title(title),
                    html.Div(
                        [
                            html.Button(
                                "Month",
                                id=f"{fig_id}-monthly",
                                n_clicks=0,
                                className="btn-toggle",
                            ),
                            html.Button(
                                "Year",
                                id=f"{fig_id}-yearly",
                                n_clicks=0,
                                className="btn-toggle",
                            ),
                        ],
                        className="card-header-buttons",
                    ),
                ],
                className="card-header-with-buttons",
            ),
            # Graph
            dcc.Graph(id=fig_id),
        ],
        className=f"card card-graph col-{width}",
    )


def make_double_figure_card_MonthYear(
    title_abs: str, fig_id_abs: str, title_pct: str, fig_id_pct: str, width: int = 6
) -> Any:
    return html.Div(
        [
            html.Div(
                [
                    make_card_title(title_abs),
                    html.Div(
                        [
                            html.Button(
                                "Month",
                                id=f"{fig_id_abs}-monthly",
                                n_clicks=0,
                                className="btn-toggle",
                            ),
                            html.Button(
                                "Year",
                                id=f"{fig_id_abs}-yearly",
                                n_clicks=0,
                                className="btn-toggle",
                            ),
                        ],
                        className="card-header-buttons",
                    ),
                ],
                className="card-header-with-buttons",
            ),
            # Plots
            dcc.Graph(id=fig_id_abs, className="subplot-spacing"),
            make_card_title(title_pct),
            dcc.Graph(id=fig_id_pct),
        ],
        className=f"card card-graph col-{width}",
    )


def make_CategoryDonut_card(
    title: str, pdf_CatMain: pd.DataFrame, width: int = 6
) -> Any:

    years_raw = pdf_CatMain["Year"].dropna().astype(str).unique()
    s_Year = sorted([year for year in years_raw if year.isdigit()], reverse=True)

    return html.Div(
        [
            html.Div(
                className="card-header",
                children=[
                    make_card_title(title),
                    dcc.Dropdown(
                        id="dropdown-Year",
                        className="dropdown-year",
                        options=[  # type: ignore[arg-type]
                            {"label": "All", "value": "All"},
                            *[
                                {"label": str(year), "value": str(year)}
                                for year in s_Year
                            ],
                        ],
                        value="All",
                        clearable=False,
                    ),
                ],
            ),
            dcc.Graph(id="fig-Donut", style={"flex": "1"}),
        ],
        className=f"card card-graph col-{width}",
    )


def make_table_card(
    title: str,
    s_col: list[dict[str, Any]],
    data: list[dict[str, Any]],
    width: int = 6,
) -> Any:

    data_head = data[:15]

    # Table header
    header = html.Thead(
        html.Tr(
            [
                html.Th(
                    col["name"], className="" if col["id"] in vis.s_Col_Text else "num"
                )
                for col in s_col
            ]
        )
    )

    # Table body
    body = html.Tbody(
        [
            html.Tr(
                [
                    html.Td(
                        (
                            f'{row[col["id"]]:,.2f}'
                            if isinstance(row[col["id"]], (int, float))
                            else row[col["id"]]
                        ),
                        className=(
                            "num balance-positive"
                            if col["id"] == "NetBalance" and row[col["id"]] > 0
                            else (
                                "num balance-negative"
                                if col["id"] == "NetBalance" and row[col["id"]] < 0
                                else (
                                    "num"
                                    if isinstance(row[col["id"]], (int, float))
                                    else ""
                                )
                            )
                        ),
                    )
                    for col in s_col
                ]
            )
            for row in data_head
        ]
    )

    return html.Div(
        [make_card_title(title), html.Table([header, body], className="simple-table")],
        className=f"card card-graph col-{width}",
    )


def make_TopCategory_card(
    title: str,
    Category: str,
    MonthLast: str,
    amount_MonthLast: float,
    amount_MonthPrev: float,
    amount_12m_avg: float,
    diff_prev_pct: float,
    diff_12m_pct: float,
    width: int = 6,
) -> Any:
    text_prev, class_prev = format_diff(diff_prev_pct)
    text_12m, class_12m = format_diff(diff_12m_pct)

    return html.Div(
        [
            make_card_title(f"{title} ({MonthLast})"),
            # Headline: Category · Amount
            html.Div(
                f"{Category} · {amount_MonthLast:,.0f} CHF", className="kpi-category"
            ),
            # Difference vs previous month
            html.Div(
                [html.Span(text_prev, className=f"kpi-value {class_prev}")],
                className="kpi-diff-row",
            ),
            html.Div(f"prev: {amount_MonthPrev:,.0f} CHF", className="kpi-subtext"),
            # Difference vs 12-month average
            html.Div(
                [html.Span(text_12m, className=f"kpi-value {class_12m}")],
                className="kpi-diff-row",
            ),
            html.Div(f"12m avg: {amount_12m_avg:,.0f} CHF", className="kpi-subtext"),
        ],
        className=f"card card-kpi col-{width}",
    )
