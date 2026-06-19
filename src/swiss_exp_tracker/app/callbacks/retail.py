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

_NPIXEL_RETAIL = 60


def register_callbacks(app: Any, data: Any) -> None:
    @app.callback(  # type: ignore[untyped-decorator]
        Output("fig-Retail", "figure"),
        Output("fig-Retail-monthly", "className"),
        Output("fig-Retail-yearly", "className"),
        Input("fig-Retail-monthly", "n_clicks"),
        Input("fig-Retail-yearly", "n_clicks"),
    )
    def update_RetailPlot(  # pyright: ignore[reportUnusedFunction]
        _n_monthly: int | None, _n_yearly: int | None
    ) -> tuple[Any, str, str]:
        trigger: str | None = cast("str | None", ctx.triggered_id)
        freq: Literal["Monthly", "Yearly"]
        if trigger == "fig-Retail-yearly":
            freq = "Yearly"
            monthly_class = "btn-toggle"
            yearly_class = "btn-toggle btn-toggle-active"
        else:
            freq = "Monthly"
            monthly_class = "btn-toggle btn-toggle-active"
            yearly_class = "btn-toggle"

        fig = F.fig_BarFreqByCategory(
            pdf=data.pdf_Retail,
            col_catgeory="category_retail",
            col_amount="Total",
            Freq=freq,
            npixel=_NPIXEL_RETAIL,
            col_map=vis.vk_Retail_col,
        )
        return fig, monthly_class, yearly_class

    @app.callback(  # type: ignore[untyped-decorator]
        Output("fig-Retail-Donut", "figure"),
        Input("dropdown-Retail-Year", "value"),
    )
    def update_RetailDonut(year: str) -> Any:  # pyright: ignore[reportUnusedFunction]
        pdf = data.pdf_RetailDonut[data.pdf_RetailDonut["Year"] == year].copy()
        return F.fig_DonutByCategory(
            pdf, "category_retail", "amount_CHF", col_map=vis.vk_Retail_col
        )
