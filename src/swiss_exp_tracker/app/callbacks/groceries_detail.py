from __future__ import annotations

from typing import Any
from typing import Literal
from typing import cast

from dash import Input
from dash import Output
from dash import State
from dash import ctx

from swiss_exp_tracker.app.vis.figure import Fig


F = Fig()


def register_callbacks(app: Any, data: Any) -> None:
    @app.callback(  # type: ignore[untyped-decorator]
        Output("gd-fig-cat", "figure"),
        Output("gd-fig-cat-monthly", "className"),
        Output("gd-fig-cat-yearly", "className"),
        Input("gd-fig-cat-monthly", "n_clicks"),
        Input("gd-fig-cat-yearly", "n_clicks"),
    )
    def update_cat_bar(  # pyright: ignore[reportUnusedFunction]
        _n_monthly: int | None, _n_yearly: int | None
    ) -> tuple[Any, str, str]:
        trigger: str | None = cast("str | None", ctx.triggered_id)
        freq: Literal["Monthly", "Yearly"]
        if trigger == "gd-fig-cat-yearly":
            freq = "Yearly"
            monthly_class = "btn-toggle"
            yearly_class = "btn-toggle btn-toggle-active"
        else:
            freq = "Monthly"
            monthly_class = "btn-toggle btn-toggle-active"
            yearly_class = "btn-toggle"

        return (
            F.fig_BarGroceryCat(data.pdf_GroceryCat, freq),
            monthly_class,
            yearly_class,
        )

    @app.callback(  # type: ignore[untyped-decorator]
        Output("gd-store-donut", "data"),
        Output("gd-fig-donut", "figure"),
        Output("gd-btn-back", "style"),
        Input("gd-fig-donut", "clickData"),
        Input("gd-btn-back", "n_clicks"),
        State("gd-store-donut", "data"),
    )
    def update_donut(  # pyright: ignore[reportUnusedFunction]
        click_data: dict[str, Any] | None,
        _n_back: int | None,
        store_data: dict[str, str] | None,
    ) -> tuple[dict[str, str] | None, Any, dict[str, str]]:
        trigger: str | None = cast("str | None", ctx.triggered_id)

        new_store: dict[str, str] | None

        if trigger == "gd-btn-back":
            if store_data is not None and "detail" in store_data:
                new_store = {"main": store_data["main"]}
            else:
                new_store = None
        elif trigger == "gd-fig-donut" and click_data is not None:
            label = str(click_data["points"][0]["label"])
            if store_data is None:
                new_store = {"main": label}
            elif "detail" not in store_data:
                new_store = {"main": store_data["main"], "detail": label}
            else:
                new_store = store_data
        else:
            new_store = store_data

        if new_store is None:
            fig = F.fig_DonutGroceryCat(data.pdf_GroceryItems)
        elif "detail" not in new_store:
            fig = F.fig_DonutGroceryCat(data.pdf_GroceryItems, new_store["main"])
        else:
            fig = F.fig_DonutGroceryCat(
                data.pdf_GroceryItems, new_store["main"], new_store["detail"]
            )

        btn_style = {"display": "inline-block"} if new_store else {"display": "none"}
        return new_store, fig, btn_style
