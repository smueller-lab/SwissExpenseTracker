from dash import Input, Output, ctx
from app.vis.figure import Fig
F = Fig()


def register_callbacks(app, data):
    @app.callback(
        Output("fig-Food", "figure"),
        Output("fig-Food-monthly", "className"),
        Output("fig-Food-yearly", "className"),
        Input("fig-Food-monthly", "n_clicks"),
        Input("fig-Food-yearly", "n_clicks"),
    )
    def update_FoodPlot(n_monthly, n_yearly):

        trigger = ctx.triggered_id

        if trigger == "fig-Food-yearly":
            freq = "Yearly"
            monthly_class = "freq-btn"
            yearly_class = "freq-btn freq-btn-active"
        else:
            freq = "Monthly"
            monthly_class = "freq-btn freq-btn-active"
            yearly_class = "freq-btn"

        fig = F.fig_BarFood(data.pdf_Food, freq)

        return fig, monthly_class, yearly_class