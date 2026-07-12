from __future__ import annotations

from typing import Any

import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]

from dash import dcc
from dash import html

import swiss_exp_tracker.app.vis.ploty_template  # pyright: ignore[reportUnusedImport] # noqa: F401  # registers myTemp Plotly template

# Shared dcc.Graph config: hide the Plotly modebar (camera/zoom/pan toolbar) on
# every chart for a cleaner dashboard look.
GRAPH_CONFIG: dcc.Graph.Config = {"displayModeBar": False, "responsive": True}


def make_empty_figure(height: float | None = None) -> go.Figure:
    """Return a themed, dataless figure for dcc.Graph's static default.

    Avoids the flash of Plotly's default white background between page mount
    and the callback that fills in the real figure. Pass the real figure's
    eventual height so the placeholder never needs to grow/shrink when the
    callback swaps it in — without a matching height, Plotly's 450px default
    can mismatch the real figure's height and leave the card needing a
    manual window resize to reflow to the correct size.
    """
    layout: dict[str, object] = {"template": "myTemp"}
    if height is not None:
        layout["height"] = height
    return go.Figure(layout=layout)


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
