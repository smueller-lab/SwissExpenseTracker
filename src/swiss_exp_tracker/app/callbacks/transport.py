from __future__ import annotations

from typing import Any
from typing import Literal
from typing import cast

from dash import Input
from dash import Output
from dash import ctx

from swiss_exp_tracker.app.config import VIS
from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()
vis = VIS()

_NPIXEL_CAR = 60


def register_callbacks(app: Any, data: Any) -> None:
    @app.callback(  # type: ignore[untyped-decorator]
        Output("fig-Car", "figure"),
        Output("fig-Car-monthly", "className"),
        Output("fig-Car-yearly", "className"),
        Input("fig-Car-monthly", "n_clicks"),
        Input("fig-Car-yearly", "n_clicks"),
    )
    def update_CarPlot(  # pyright: ignore[reportUnusedFunction]
        _n_monthly: int | None, _n_yearly: int | None
    ) -> tuple[Any, str, str]:
        trigger: str | None = cast("str | None", ctx.triggered_id)
        freq: Literal["Monthly", "Yearly"]
        if trigger == "fig-Car-yearly":
            freq = "Yearly"
            monthly_class = "btn-toggle"
            yearly_class = "btn-toggle btn-toggle-active"
        else:
            freq = "Monthly"
            monthly_class = "btn-toggle btn-toggle-active"
            yearly_class = "btn-toggle"

        fig = F.fig_BarFreqByCategory(
            pdf=data.pdf_Car,
            col_catgeory="category_car",
            col_amount="Total",
            Freq=freq,
            npixel=_NPIXEL_CAR,
            col_map=vis.vk_Car_col,
        )
        return fig, monthly_class, yearly_class
