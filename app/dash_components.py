from __future__ import annotations

from typing import Any

from dash import html


def make_page_title(title: str) -> Any:
    return html.Div(
        [html.H2(title, className="page-title-center")], style={"width": "100%"}
    )


def make_card_title(title: str) -> Any:
    return html.H6(title, className="card-title")


def get_balance_class(value: float) -> str:
    return "kpi-value kpi-positive" if value > 0 else "kpi-value kpi-negative"


def format_diff(pct: float) -> tuple[str, str]:
    sign = "+" if pct >= 0 else "-"
    color = "kpi-negative" if pct >= 0 else "kpi-positive"
    return f"{sign}{abs(pct):.1f} %", color
