from dash import html
from app.dash_components import make_page_title
from app.components.cards import make_figure_card_MonthYear
from app.vis.figure import Fig
from app.config import config
F = Fig()
cfg = config()

def layout(data):
    return html.Div([

        make_page_title("⛳ Sport Analytics"),

        html.Div([
            make_figure_card_MonthYear('Sport expenses [CHF]', 'fig-Sport', width=12)
        ], className="grid")

    ])