from dash import html
from app.components.cards import make_figure_card_MonthYear, make_figure_card
from app.vis.figure import Fig
F = Fig()

def layout(data):
    return html.Div([

        html.Div([
            html.H2("🍽️ Dining Analytics", className="page-title-center")
        ], style={"width": "100%"}),

        html.Div([
            make_figure_card_MonthYear('Food & Dining expenses [CHF]', 'fig-Food', width=12),
            make_figure_card('Food & Dining expenses per visit [CHF]', F.fig_BoxFood(data.pdf_Master), width=12)
        ], className="grid")
    ])


