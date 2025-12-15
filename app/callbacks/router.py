from dash import Input, Output
from app.layout.home import layout as home_layout
from app.layout.food import layout as food_layout
from app.layout.groceries import layout as groceries_layout


def register_callbacks(app, data):

    @app.callback(
        Output("page-content", "children"),
        Input("url", "pathname")
    )
    def display_page(pth):
        if pth == "/":
            return home_layout(data)
        elif pth == "/groceries":
            return groceries_layout(data)
        elif pth == "/food":
            return food_layout(data)
        return home_layout(data)


    @app.callback(
        [
            Output("link-home", "className"),
            Output("link-groceries", "className"),
            Output("link-food", "className"),
            Output("link-transport", "className"),
            Output("link-sport", "className"),
        ],
        Input("url", "pathname")
    )
    def highlight_active_tab(pth):
        default = "menu-link"
        active = "menu-link active"
        return [
            active if pth == "/" else default,
            active if pth == "/groceries" else default,
            active if pth == "/food" else default,
            active if pth == "/transport" else default,
            active if pth == "/sport" else default,
        ]
