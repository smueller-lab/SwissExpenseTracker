from __future__ import annotations

from typing import Any

from dash import dcc
from dash import html

# Shared dcc.Graph config: hide the Plotly modebar (camera/zoom/pan toolbar) on
# every chart for a cleaner dashboard look.
GRAPH_CONFIG: dcc.Graph.Config = {"displayModeBar": False, "responsive": True}


def make_page_title(title: str) -> Any:
    return html.Div(
        [html.H2(title, className="page-title-center")], className="page-title-wrap"
    )


def make_card_title(title: str) -> Any:
    return html.H6(title, className="card-title")


def get_balance_class(value: float | None) -> str:
    if value is None:
        return "kpi-value"
    return "kpi-value kpi-positive" if value > 0 else "kpi-value kpi-negative"


def format_diff(pct: float) -> tuple[str, str]:
    sign = "+" if pct >= 0 else "-"
    color = "kpi-negative" if pct >= 0 else "kpi-positive"
    return f"{sign}{abs(pct):.1f} %", color
