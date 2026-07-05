from __future__ import annotations

from typing import Any

from dash import dcc
from dash import html


def create_app_layout(
    data_through: str, n_transactions: int, app_version: str = ""
) -> Any:
    footer_children = [
        html.Div(f"Data through {data_through}", className="sidebar-footer-line"),
        html.Div(f"{n_transactions:,} transactions", className="sidebar-footer-sub"),
        html.Div(className="sidebar-footer-divider"),
        html.Div("Swiss Expense Tracker", className="sidebar-footer-name"),
    ]
    if app_version:
        footer_children.append(
            html.Div(f"v{app_version}", className="sidebar-footer-version")
        )

    return html.Div(
        [
            # ---------------- Sidebar ----------------
            html.Div(
                [
                    html.H2("Expense Tracker", className="logo"),
                    dcc.Link(
                        "🏠 Home", href="/", id="link-home", className="menu-link"
                    ),
                    dcc.Link(
                        "📊 Balance Sheet",
                        href="/balance-sheet",
                        id="link-balance-sheet",
                        className="menu-link",
                    ),
                    dcc.Link(
                        "\U0001f4b0 Budget / Forecasting",
                        href="/budget-forecast",
                        id="link-budget-forecast",
                        className="menu-link",
                    ),
                    dcc.Link(
                        "🛒 Groceries",
                        href="/groceries",
                        id="link-groceries",
                        className="menu-link",
                    ),
                    dcc.Link(
                        [
                            html.Span("M", className="migros-m"),
                            " Cumulus",
                        ],
                        href="/m-cumulus",
                        id="link-m-cumulus",
                        className="menu-link",
                    ),
                    dcc.Link(
                        "🍽️ Dining & Bars",
                        href="/food",
                        id="link-food",
                        className="menu-link",
                    ),
                    dcc.Link(
                        "🏖️ Vacation",
                        href="/vacation",
                        id="link-vacation",
                        className="menu-link",
                    ),
                    dcc.Link(
                        "🚄 Transport",
                        href="/transport",
                        id="link-transport",
                        className="menu-link",
                    ),
                    dcc.Link(
                        "⛳ Sport",
                        href="/sport",
                        id="link-sport",
                        className="menu-link",
                    ),
                    dcc.Link(
                        "🛍️ Retail",
                        href="/retail",
                        id="link-retail",
                        className="menu-link",
                    ),
                    dcc.Link(
                        "🔍 Smart Table",
                        href="/smarttable",
                        id="link-smarttable",
                        className="menu-link",
                    ),
                    dcc.Link(
                        "📈 Investing",
                        href="/investing",
                        id="link-investing",
                        className="menu-link",
                    ),
                    html.Div(footer_children, className="sidebar-footer"),
                ],
                className="sidebar",
            ),
            # ---------------- Router ----------------
            dcc.Location(id="url", refresh=False),
            # ---------------- Page content ----------------
            html.Div(id="page-content", className="content"),
        ]
    )
