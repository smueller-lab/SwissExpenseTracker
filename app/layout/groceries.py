from dash import html
from app.dash_components import make_page_title
from app.components.cards import make_double_figure_card_MonthYear, make_figure_card
from app.vis.figure import Fig
F = Fig()


def layout(data):
    return html.Div([

        make_page_title("🛒 Grocery store Analytics"),
    
        html.Div([
            make_double_figure_card_MonthYear(
                title_abs='Grocery store expenses [CHF]',
                fig_id_abs='fig-Abs',
                title_pct='Grocery store expenses [%]',
                fig_id_pct='fig-Pct',
                width=12
            ),
            make_figure_card('Grocery store expenses per visit [CHF]', F.fig_BoxGrocery(data.pdf_Master), width=12)
        ], className="grid")
    ])