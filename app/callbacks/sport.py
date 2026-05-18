from __future__ import annotations

from typing import Any
from typing import Literal

from dash import Input
from dash import Output
from dash import ctx

from app.config import config
from app.vis.figure import Fig


F = Fig()
cfg = config()


def register_callbacks(app: Any, data: Any) -> None:
    @app.callback(  # type: ignore[untyped-decorator]
        Output("fig-Sport", "figure"),
        Output("fig-Sport-monthly", "className"),
        Output("fig-Sport-yearly", "className"),
        Input("fig-Sport-monthly", "n_clicks"),
        Input("fig-Sport-yearly", "n_clicks"),
    )
    def update_SportPlot(
        n_monthly: int | None, n_yearly: int | None
    ) -> tuple[Any, str, str]:
        trigger = ctx.triggered_id
        freq: Literal["Monthly", "Yearly"]
        if trigger == "fig-Sport-yearly":
            freq = "Yearly"
            monthly_class = "btn-toggle"
            yearly_class = "btn-toggle btn-toggle-active"
        else:
            freq = "Monthly"
            monthly_class = "btn-toggle btn-toggle-active"
            yearly_class = "btn-toggle"

        fig = F.fig_BarFreqByCategory(
            pdf=data.pdf_Sport,
            col_catgeory="category_sport",
            col_amount="Total",
            Freq=freq,
            dTick=cfg.vk_dTick_Sport[freq],
            npixel=cfg.vk_npixel_Sport[freq],
        )
        return fig, monthly_class, yearly_class
