from dash import html
from app.components.cards import make_double_figure_card_MonthYear, make_figure_card
from app.vis.figure import Fig
F = Fig()


def layout(data):
    return html.Div([
        html.H2("🛒 Grocery store Analytics", className="page-title-center"),
        make_double_figure_card_MonthYear('Grocery store expenses [CHF / %]', 'fig-Abs', 'fig-Pct'),
        make_figure_card('Grocery store expenses per visit [CHF]', F.fig_BoxGrocery(data.pdf_Master))
    ])