from __future__ import annotations

from typing import Any
from typing import Literal
from typing import cast

from dash import Input
from dash import Output
from dash import ctx

from swiss_exp_tracker.app.vis.figure import Fig


F = Fig()


def register_callbacks(app: Any, data: Any) -> None:
    @app.callback(  # type: ignore[untyped-decorator]
        Output("fig-Abs", "figure"),
        Output("fig-Pct", "figure"),
        Output("fig-Abs-monthly", "className"),
        Output("fig-Abs-yearly", "className"),
        Input("fig-Abs-monthly", "n_clicks"),
        Input("fig-Abs-yearly", "n_clicks"),
    )
    def update_GroceryDualPlot(  # pyright: ignore[reportUnusedFunction]
        _n_monthly: int | None, _n_yearly: int | None
    ) -> tuple[Any, Any, str, str]:
        trigger: str | None = cast("str | None", ctx.triggered_id)
        freq: Literal["Monthly", "Yearly"]
        if trigger == "fig-Abs-yearly":
            freq = "Yearly"
            monthly_class = "btn-toggle"
            yearly_class = "btn-toggle btn-toggle-active"
        else:
            freq = "Monthly"
            monthly_class = "btn-toggle btn-toggle-active"
            yearly_class = "btn-toggle"

        fig_abs = F.fig_BarGrocery(data.pdf_Grocery, freq)
        fig_pct = F.fig_BarGrocery_pct(data.pdf_Grocery, freq)
        return fig_abs, fig_pct, monthly_class, yearly_class
