from dash import Input, Output
from app.vis.figure import Fig
F = Fig()

def register_callbacks(app, data):

    @app.callback(
        Output("fig-Donut", "figure"),
        Input("dropdown-Year", "value")
    )
    def update_Donut(Period):
        pdf_CatMain = data.pdf_CatMain.copy()
        pdf_Period = pdf_CatMain[pdf_CatMain['Year'] == Period].copy()

        fig = F.fig_DonutCategoryMain(pdf_Period)

        return fig