from __future__ import annotations

from typing import Any

from dash import Input
from dash import Output

from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()


def register_callbacks(app: Any, data: Any) -> None:
    @app.callback(  # type: ignore[untyped-decorator]
        Output("fig-Donut", "figure"), Input("dropdown-Year", "value")
    )
    def update_Donut(Period: str) -> Any:  # pyright: ignore[reportUnusedFunction]
        pdf_CatMain = data.pdf_CatMain.copy()
        pdf_Period = pdf_CatMain[pdf_CatMain["Year"] == Period].copy()
        return F.fig_DonutCategoryMain(pdf_Period)
