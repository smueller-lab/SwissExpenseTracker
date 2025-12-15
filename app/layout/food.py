from dash import html
from app.components.cards import make_figure_card_MonthYear, make_figure_card
from app.vis.figure import Fig
F = Fig()


def layout(data):
    return html.Div([
        html.H2("🍽️ Dining Analytics", className="page-title-center"),
        make_figure_card_MonthYear('Food & Dining expenses [CHF]', 'fig-Food'),
        make_figure_card('Food & Dining expenses per visit [CHF]', F.fig_BoxFood(data.pdf_Master))
    ])