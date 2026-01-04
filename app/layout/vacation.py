from dash import html
from app.dash_components import make_page_title
from app.components.cards import make_figure_card
from app.vis.figure import Fig
F = Fig()

def layout(data):
    return html.Div([

        make_page_title("🏖️ Vacation Analytics"),

        html.Div([
            make_figure_card('Vacation expenses [CHF]', F.fig_BarVacation(data.pdf_Vacation), width=12)
        ], className="grid")
    ])