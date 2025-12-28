from dash import Input, Output, ctx
from app.vis.figure import Fig
F = Fig()


def register_callbacks(app, data):

    @app.callback(
        Output("fig-Abs", "figure"),
        Output("fig-Pct", "figure"),
        Output("fig-Abs-monthly", "className"),
        Output("fig-Abs-yearly", "className"),
        Input("fig-Abs-monthly", "n_clicks"),
        Input("fig-Abs-yearly", "n_clicks"),
    )
    def update_GroceryDualPlot(n_monthly, n_yearly):

        trigger = ctx.triggered_id

        if trigger == "fig-Abs-yearly":
            freq = "Yearly"
            monthly_class = "btn-toggle"
            yearly_class = "btn-toggle btn-toggle-active"
        else:
            freq = "Monthly"
            monthly_class = "btn-toggle btn-toggle-active"
            yearly_class = "btn-toggle"

        fig_abs = F.fig_BarGrocery(data.pdf_Grocery, freq)
        fig_pct = F.fig_BarGrocery_pct(data.pdf_Grocery, freq)

        return fig_abs, fig_pct, monthly_class, yearly_class