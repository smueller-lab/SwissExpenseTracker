from dash import Input, Output, ctx
from app.vis.figure import Fig
from app.config import config
F = Fig()
cfg = config()

def register_callbacks(app, data):
    @app.callback(
        Output("fig-Sport", "figure"),
        Output("fig-Sport-monthly", "className"),
        Output("fig-Sport-yearly", "className"),
        Input("fig-Sport-monthly", "n_clicks"),
        Input("fig-Sport-yearly", "n_clicks"),
    )
    def update_SportPlot(n_monthly, n_yearly):

        trigger = ctx.triggered_id

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
            col_catgeory='category_sport',
            col_amount='Total',
            Freq=freq,
            dTick=cfg.vk_dTick_Sport[freq],
            npixel=cfg.vk_npixel_Sport[freq]
        )

        return fig, monthly_class, yearly_class