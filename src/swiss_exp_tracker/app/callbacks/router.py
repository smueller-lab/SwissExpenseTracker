from __future__ import annotations

from typing import Any

from dash import Input
from dash import Output

from swiss_exp_tracker.app.layout.food import layout as food_layout
from swiss_exp_tracker.app.layout.groceries import layout as groceries_layout
from swiss_exp_tracker.app.layout.home import layout as home_layout
from swiss_exp_tracker.app.layout.retail import layout as retail_layout
from swiss_exp_tracker.app.layout.smarttable import layout as smarttable_layout
from swiss_exp_tracker.app.layout.sport import layout as sport_layout
from swiss_exp_tracker.app.layout.transport import layout as transport_layout
from swiss_exp_tracker.app.layout.vacation import layout as vacation_layout


def register_callbacks(app: Any, data: Any) -> None:
    @app.callback(  # type: ignore[untyped-decorator]
        Output("page-content", "children"), Input("url", "pathname")
    )
    def display_page(pth: str) -> Any:  # pyright: ignore[reportUnusedFunction]
        if pth == "/":
            return home_layout(data)
        elif pth == "/groceries":
            return groceries_layout(data)
        elif pth == "/food":
            return food_layout(data)
        elif pth == "/vacation":
            return vacation_layout(data)
        elif pth == "/transport":
            return transport_layout(data)
        elif pth == "/sport":
            return sport_layout(data)
        elif pth == "/retail":
            return retail_layout(data)
        elif pth == "/smarttable":
            return smarttable_layout(data)
        return home_layout(data)

    @app.callback(  # type: ignore[untyped-decorator]
        [
            Output("link-home", "className"),
            Output("link-groceries", "className"),
            Output("link-food", "className"),
            Output("link-vacation", "className"),
            Output("link-transport", "className"),
            Output("link-sport", "className"),
            Output("link-retail", "className"),
            Output("link-smarttable", "className"),
        ],
        Input("url", "pathname"),
    )
    def highlight_active_tab(  # pyright: ignore[reportUnusedFunction]
        pth: str,
    ) -> list[str]:
        default = "menu-link"
        active = "menu-link active"
        return [
            active if pth == "/" else default,
            active if pth == "/groceries" else default,
            active if pth == "/food" else default,
            active if pth == "/vacation" else default,
            active if pth == "/transport" else default,
            active if pth == "/sport" else default,
            active if pth == "/retail" else default,
            active if pth == "/smarttable" else default,
        ]
