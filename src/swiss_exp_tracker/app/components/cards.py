from __future__ import annotations

import math

from typing import Any

import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]

from dash import dcc
from dash import html

from swiss_exp_tracker.app.config import VIS
from swiss_exp_tracker.app.dash_components import GRAPH_CONFIG
from swiss_exp_tracker.app.dash_components import format_diff
from swiss_exp_tracker.app.dash_components import get_balance_class
from swiss_exp_tracker.app.dash_components import make_card_title

vis = VIS()


def make_number_card(
    title: str,
    Number: float | str | None,
    width: int = 3,
    unit: str = "CHF",
    fmt: str = ",.2f",
    value_class: str | None = None,
) -> Any:
    if isinstance(Number, str):
        cls = value_class if value_class is not None else get_balance_class(None)
        text = Number
        return html.Div(
            [make_card_title(title), html.P(text, className=cls)],
            className=f"card card-kpi col-{width}",
        )
    is_missing = Number is None or math.isnan(Number)
    cls = (
        value_class
        if value_class is not None
        else get_balance_class(None if is_missing else Number)
    )
    if is_missing:
        text = "—"
    else:
        suffix = f" {unit}" if unit else ""
        text = f"{Number:{fmt}}{suffix}"
    return html.Div(
        [
            make_card_title(title),
            html.P(text, className=cls),
        ],
        className=f"card card-kpi col-{width}",
    )


def make_figure_card(title: str, fig: go.Figure, width: int = 6) -> Any:
    return html.Div(
        [make_card_title(title), dcc.Graph(figure=fig, config=GRAPH_CONFIG)],
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
            dcc.Graph(id=fig_id, config=GRAPH_CONFIG),
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
            dcc.Graph(id=fig_id_abs, className="subplot-spacing", config=GRAPH_CONFIG),
            make_card_title(title_pct),
            dcc.Graph(id=fig_id_pct, config=GRAPH_CONFIG),
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
            dcc.Graph(id="fig-Donut", className="graph-flex", config=GRAPH_CONFIG),
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
                    col["name"],
                    className=col.get(
                        "header_class", "" if col["id"] in vis.s_Col_Text else "num"
                    ),
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


def make_balance_sheet_card(
    title: str,
    pdf: pd.DataFrame,
    width: int = 12,
) -> Any:
    """Return a full-width card with the year x metric balance-sheet matrix.

    total_plus and yoy_saved_pct cells are colored balance-positive/negative;
    rate columns carry a '%' suffix; CHF columns are formatted ,.0f.
    """
    _COLS: list[dict[str, Any]] = [
        {"id": "year", "name": "Year", "fmt": "text", "color": False},
        {"id": "income", "name": "Income", "fmt": "chf", "color": False},
        {"id": "expense", "name": "Spent", "fmt": "chf", "color": False},
        {"id": "invested", "name": "Invested", "fmt": "chf", "color": False},
        {"id": "total_plus", "name": "Net Gain", "fmt": "chf", "color": True},
        {"id": "savings_rate_pct", "name": "Gain %", "fmt": "pct", "color": True},
        {"id": "expense_rate_pct", "name": "Exp %", "fmt": "pct", "color": False},
        {"id": "yoy_saved_pct", "name": "NetGain YoY %", "fmt": "pct", "color": True},
        {"id": "in_plus", "name": "Result", "fmt": "res", "color": False},
    ]

    header = html.Thead(
        html.Tr(
            [
                html.Th(
                    col["name"],
                    className="" if col["fmt"] == "text" else "num",
                )
                for col in _COLS
            ]
        )
    )

    def _render_cell(col: dict[str, Any], row: Any) -> Any:
        """Return a styled html.Td for a single balance-sheet matrix cell."""
        val: Any = row[col["id"]]
        fmt: str = col["fmt"]
        colored: bool = bool(col["color"])
        missing: bool = bool(pd.isna(val))

        if fmt == "text":
            return html.Td(str(int(val)) if not missing else "—", className="")

        if fmt == "res":
            pos = (not missing) and int(val) == 1
            return html.Td(
                "▲ +" if pos else "▼ -",
                className="num balance-positive" if pos else "num balance-negative",
            )

        if fmt == "chf":
            text = f"{val:,.0f}" if not missing else "—"
        else:
            text = f"{val:.1f} %" if not missing else "—"

        if colored and not missing:
            if val > 0:
                cls = "num balance-positive"
            elif val < 0:
                cls = "num balance-negative"
            else:
                cls = "num"
        else:
            cls = "num"

        return html.Td(text, className=cls)

    body = html.Tbody(
        [
            html.Tr([_render_cell(col, row) for col in _COLS])
            for _, row in pdf.iterrows()
        ]
    )

    return html.Div(
        [make_card_title(title), html.Table([header, body], className="simple-table")],
        className=f"card card-graph col-{width}",
    )


def make_category_spend_card(
    title: str,
    pdf_pivot: pd.DataFrame,
    pdf_yoy: pd.DataFrame,
    width: int = 12,
) -> Any:
    """Return a full-width card with the major-category spend pivot table.

    Rows are years descending; each cell shows the yearly spend and a colored YoY %
    delta — balance-negative when spend rose, balance-positive when spend fell.
    """
    cat_cols = [c for c in pdf_pivot.columns if c != "year"]

    header = html.Thead(
        html.Tr(
            [html.Th("Year", className="")]
            + [html.Th(cat, className="num") for cat in cat_cols]
        )
    )

    def _cat_cell(cat: str, pivot_row: Any, yoy_row: Any) -> Any:
        """Return a styled html.Td containing the spend amount and YoY delta."""
        amount: Any = pivot_row[cat] if cat in pivot_row.index else 0.0
        yoy_val: Any = yoy_row[cat] if cat in yoy_row.index else None
        amt_text = f"{amount:,.0f}" if pd.notna(amount) else "—"

        if pd.isna(yoy_val):
            return html.Td(
                [
                    html.Span(amt_text),
                    html.Br(),
                    html.Span("—", className="num cat-delta"),
                ],
                className="num",
            )

        # Infinite YoY (prior year was 0) is not meaningful — show no delta.
        if math.isinf(yoy_val):
            return html.Td(
                [
                    html.Span(amt_text),
                    html.Br(),
                    html.Span("", className="num cat-delta"),
                ],
                className="num",
            )

        sign = "+" if yoy_val >= 0 else ""
        delta_text = f"{sign}{yoy_val:.1f} %"
        delta_cls = (
            "num cat-delta balance-negative"
            if yoy_val > 0
            else "num cat-delta balance-positive"
        )
        return html.Td(
            [
                html.Span(amt_text),
                html.Br(),
                html.Span(delta_text, className=delta_cls),
            ],
            className="num",
        )

    pivot_rows = list(pdf_pivot.iterrows())
    yoy_rows = list(pdf_yoy.iterrows())

    body_rows: list[Any] = []
    for i, (_, pivot_row) in enumerate(pivot_rows):
        year_val: Any = (
            pivot_row["year"] if "year" in pivot_row.index else pivot_row.name
        )
        year_str = str(int(year_val)) if pd.notna(year_val) else "—"
        yoy_row: Any = (
            yoy_rows[i][1] if i < len(yoy_rows) else pd.Series(dtype="float64")
        )

        body_rows.append(
            html.Tr(
                [html.Td(year_str, className="")]
                + [_cat_cell(cat, pivot_row, yoy_row) for cat in cat_cols]
            )
        )

    body = html.Tbody(body_rows)

    return html.Div(
        [
            make_card_title(title),
            html.Table([header, body], className="simple-table category-spend-table"),
        ],
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
