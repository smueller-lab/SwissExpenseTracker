from __future__ import annotations

from typing import Any
from typing import Literal
from typing import cast

from dash import Input
from dash import Output
from dash import ctx

from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()
cfg = config()

_NPIXEL_SPORT = 60


def register_callbacks(app: Any, data: Any) -> None:
    @app.callback(  # type: ignore[untyped-decorator]
        Output("fig-Sport", "figure"),
        Output("fig-Sport-monthly", "className"),
        Output("fig-Sport-yearly", "className"),
        Input("fig-Sport-monthly", "n_clicks"),
        Input("fig-Sport-yearly", "n_clicks"),
    )
    def update_SportPlot(  # pyright: ignore[reportUnusedFunction]
        _n_monthly: int | None, _n_yearly: int | None
    ) -> tuple[Any, str, str]:
        trigger: str | None = cast("str | None", ctx.triggered_id)
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
            npixel=_NPIXEL_SPORT,
        )
        fig.update_layout(height=cfg.height_Sport)
        return fig, monthly_class, yearly_class
