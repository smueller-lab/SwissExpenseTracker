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
        Output("fig-Food", "figure"),
        Output("fig-Food-monthly", "className"),
        Output("fig-Food-yearly", "className"),
        Input("fig-Food-monthly", "n_clicks"),
        Input("fig-Food-yearly", "n_clicks"),
    )
    def update_FoodPlot(  # pyright: ignore[reportUnusedFunction]
        _n_monthly: int | None, _n_yearly: int | None
    ) -> tuple[Any, str, str]:
        trigger: str | None = cast("str | None", ctx.triggered_id)
        freq: Literal["Monthly", "Yearly"]
        if trigger == "fig-Food-yearly":
            freq = "Yearly"
            monthly_class = "btn-toggle"
            yearly_class = "btn-toggle btn-toggle-active"
        else:
            freq = "Monthly"
            monthly_class = "btn-toggle btn-toggle-active"
            yearly_class = "btn-toggle"

        fig = F.fig_BarFood(data.pdf_Food, freq)
        return fig, monthly_class, yearly_class
