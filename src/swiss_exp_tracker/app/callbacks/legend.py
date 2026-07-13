from __future__ import annotations

from typing import Any

from dash import ALL
from dash import ClientsideFunction
from dash import Input
from dash import Output


def register_callbacks(app: Any) -> None:
    """Register the clientside rescan that repositions legends on newly mounted graphs.

    Fires whenever any {"type": "chart-graph", ...} component mounts or its
    figure changes — Dash re-evaluates pattern-matching ALL callbacks when the
    matching component set changes, not just on prop updates, so this covers
    both static figures (mount once) and callback-filled ones (mount + every
    update). See assets/mobile_legend.js for the JS-side rescan implementation.
    """
    app.clientside_callback(
        ClientsideFunction(namespace="legend", function_name="rescan"),
        Output("legend-ping", "data"),
        Input({"type": "chart-graph", "index": ALL}, "figure"),
        prevent_initial_call=False,
    )
