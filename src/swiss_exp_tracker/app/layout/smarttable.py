from __future__ import annotations

from typing import Any

from dash import dash_table
from dash import dcc
from dash import html

from swiss_exp_tracker.app.dash_components import make_page_title


def layout(data: Any) -> Any:
    pdf = data.pdf_Master
    pdf_expense = pdf[pdf["transaction_type"] == "EXPENSE"]

    s_years = sorted(
        pdf_expense["date"].dropna().dt.year.astype(int).unique().tolist(),
        reverse=True,
    )
    year_options = [{"label": str(y), "value": y} for y in s_years]

    cat_main_options = [
        {"label": c, "value": c}
        for c in sorted(pdf_expense["category_main"].dropna().unique().tolist())
    ]

    amount_max = int(pdf_expense["amount_CHF"].max()) + 1

    month_options = [
        {"label": m, "value": i}
        for i, m in enumerate(
            [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
            start=1,
        )
    ]

    return html.Div(
        [
            make_page_title("🔍 Smart Table"),
            html.Div(
                [
                    # ---- Filter bar ----
                    html.Div(
                        [
                            # Row 1: Date range
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.Label(
                                                "Date range", className="filter-label"
                                            ),
                                            html.Div(
                                                [
                                                    html.Div(
                                                        [
                                                            html.Span(
                                                                "From",
                                                                className="filter-sublabel",
                                                            ),
                                                            html.Div(
                                                                [
                                                                    dcc.Dropdown(
                                                                        id="smart-year-start",
                                                                        options=year_options,  # type: ignore[arg-type]
                                                                        value=s_years[
                                                                            -1
                                                                        ],
                                                                        clearable=False,
                                                                        className="filter-dropdown filter-year-dd",
                                                                    ),
                                                                    dcc.Dropdown(
                                                                        id="smart-month-start",
                                                                        options=month_options,  # type: ignore[arg-type]
                                                                        placeholder="Any",
                                                                        clearable=True,
                                                                        className="filter-dropdown filter-month-dd",
                                                                    ),
                                                                ],
                                                                className="filter-year-month",
                                                            ),
                                                        ],
                                                        className="filter-date-item",
                                                    ),
                                                    html.Span(
                                                        "→", className="filter-arrow"
                                                    ),
                                                    html.Div(
                                                        [
                                                            html.Span(
                                                                "To",
                                                                className="filter-sublabel",
                                                            ),
                                                            html.Div(
                                                                [
                                                                    dcc.Dropdown(
                                                                        id="smart-year-end",
                                                                        options=year_options,  # type: ignore[arg-type]
                                                                        value=s_years[
                                                                            0
                                                                        ],
                                                                        clearable=False,
                                                                        className="filter-dropdown filter-year-dd",
                                                                    ),
                                                                    dcc.Dropdown(
                                                                        id="smart-month-end",
                                                                        options=month_options,  # type: ignore[arg-type]
                                                                        placeholder="Any",
                                                                        clearable=True,
                                                                        className="filter-dropdown filter-month-dd",
                                                                    ),
                                                                ],
                                                                className="filter-year-month",
                                                            ),
                                                        ],
                                                        className="filter-date-item",
                                                    ),
                                                ],
                                                className="filter-date-range",
                                            ),
                                        ],
                                        className="filter-group filter-group-date",
                                    ),
                                    dcc.Checklist(
                                        id="smart-exclude-check",
                                        options=[  # type: ignore[arg-type]
                                            {
                                                "label": " Exclude non-expense transactions",
                                                "value": "exclude",
                                            }
                                        ],
                                        value=[],
                                        className="filter-checkbox",
                                    ),
                                ],
                                className="filter-row",
                            ),
                            # Row 2: Category, Subcategory, Merchant
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.Label(
                                                "Category", className="filter-label"
                                            ),
                                            dcc.Dropdown(
                                                id="smart-cat-main",
                                                options=cat_main_options,  # type: ignore[arg-type]
                                                multi=True,
                                                placeholder="All categories…",
                                                className="filter-dropdown",
                                            ),
                                        ],
                                        className="filter-group",
                                    ),
                                    html.Div(
                                        [
                                            html.Label(
                                                "Subcategory", className="filter-label"
                                            ),
                                            dcc.Dropdown(
                                                id="smart-cat-second",
                                                options=[],
                                                multi=True,
                                                placeholder="All subcategories…",
                                                className="filter-dropdown",
                                            ),
                                        ],
                                        className="filter-group",
                                    ),
                                    html.Div(
                                        [
                                            html.Label(
                                                "Merchant", className="filter-label"
                                            ),
                                            dcc.Dropdown(
                                                id="smart-merchant",
                                                options=[],
                                                multi=True,
                                                searchable=True,
                                                placeholder="Type to search merchants…",
                                                className="filter-dropdown",
                                            ),
                                        ],
                                        className="filter-group",
                                    ),
                                ],
                                className="filter-row",
                            ),
                            # Row 3: Amount range
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.Label(
                                                "Amount range (CHF)",
                                                className="filter-label",
                                            ),
                                            dcc.RangeSlider(
                                                id="smart-amount-range",
                                                min=0,
                                                max=amount_max,
                                                step=10,
                                                value=[0, amount_max],
                                                marks={
                                                    0: "0",
                                                    500: "500",
                                                    1000: "1k",
                                                    5000: "5k",
                                                    amount_max: str(amount_max),
                                                },
                                                tooltip={
                                                    "placement": "bottom",
                                                    "always_visible": False,
                                                },
                                                className="filter-slider",
                                            ),
                                        ],
                                        className="filter-group",
                                    ),
                                ],
                                className="filter-row",
                            ),
                        ],
                        className="smart-filter-bar",
                    ),
                    # ---- Summary row ----
                    html.Div(id="smart-summary", className="smart-summary"),
                    # ---- Table ----
                    dash_table.DataTable(  # type: ignore[attr-defined]
                        id="smart-table",
                        columns=[
                            {"name": "Date", "id": "Date", "type": "text"},
                            {"name": "Merchant", "id": "Merchant", "type": "text"},
                            {
                                "name": "Category",
                                "id": "category_main",
                                "type": "text",
                            },
                            {
                                "name": "Subcategory",
                                "id": "category_second",
                                "type": "text",
                            },
                            {
                                "name": "Amount (CHF)",
                                "id": "amount_CHF",
                                "type": "numeric",
                                "format": {
                                    "specifier": ",.2f",
                                },
                            },
                        ],
                        data=[],
                        sort_action="native",
                        page_action="native",
                        page_size=50,
                        style_table={"overflowX": "auto"},
                        style_header={
                            "backgroundColor": "#1e1e2e",
                            "color": "#cdd6f4",
                            "fontWeight": "bold",
                            "borderBottom": "2px solid #45475a",
                            "textAlign": "left",
                            "padding": "10px 14px",
                        },
                        style_cell={
                            "backgroundColor": "#181825",
                            "color": "#cdd6f4",
                            "border": "none",
                            "borderBottom": "1px solid #313244",
                            "padding": "8px 14px",
                            "fontFamily": "inherit",
                            "fontSize": "13px",
                            "textAlign": "left",
                            "whiteSpace": "normal",
                        },
                        style_cell_conditional=[
                            {
                                "if": {"column_id": "amount_CHF"},
                                "textAlign": "right",  # pyright: ignore[reportArgumentType]
                            }
                        ],
                        style_data_conditional=[  # pyright: ignore[reportArgumentType]
                            {
                                "if": {"row_index": "odd"},
                                "backgroundColor": "#1e1e2e",
                            },
                            {
                                "if": {
                                    "filter_query": "{amount_CHF} >= 500",
                                    "column_id": "amount_CHF",
                                },
                                "color": "#fab387",
                                "fontWeight": "bold",
                            },
                        ],
                        style_as_list_view=True,
                    ),
                ],
                className="card card-graph col-12",
            ),
        ]
    )
