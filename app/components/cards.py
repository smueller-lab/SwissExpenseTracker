from dash import html, dcc, dash_table
import plotly.graph_objects as go
import pandas as pd


def get_balance_class(value):
    return "card-number balance-positive" if value > 0 else "card-number balance-negative"


def make_number_card(title: str, Number: float):
    return html.Div([
        html.H6(title, className='card-title'),
        html.P(f'{Number:,.2f} CHF', className=get_balance_class(Number))
    ], className='card')


def make_figure_card(title: str, fig: go.Figure):
    return html.Div([
        html.H6(title, className="graph-title"),
        dcc.Graph(figure=fig)
    ], className="graph-card-enhanced")


def make_figure_card_MonthYear(title: str, fig_id: str):
    return html.Div([
        html.Div([
            # Header Row (buttons + title + spacer)
            html.Div([
                html.Div([
                    html.Button("Month", id=f"{fig_id}-monthly", n_clicks=0, className="freq-btn"),
                    html.Button("Year", id=f"{fig_id}-yearly", n_clicks=0, className="freq-btn"),
                ], className="button-row"),

                html.H6(title, className='graph-title'),
                html.Div(className='header-spacer'),
            ], className='header-row'),

            # Graph
            dcc.Graph(id=fig_id),

        ], className='graph-container graph-card-enhanced'),
    ])


def make_double_figure_card_MonthYear(title: str, fig_id_abs: str, fig_id_pct: str):
    return html.Div([
        html.Div([
            html.Div([
                html.Button("Month", id=f"{fig_id_abs}-monthly", n_clicks=0, className="freq-btn"),
                html.Button("Year", id=f"{fig_id_abs}-yearly", n_clicks=0, className="freq-btn"),
            ], className="button-row"),

            html.H6(title, className="graph-title"),
            html.Div(className="header-spacer"),

        ], className="header-row"),

        # Absolute plot
        html.Div([
            dcc.Graph(id=fig_id_abs)
        ], className="sub-plot-container"),

        # Percentage plot
        html.Div([
            dcc.Graph(id=fig_id_pct)
        ], className="sub-plot-container"),

    ], className="big-plot-card")


def make_card_selectBox(title: str, pdf_CatMain: pd.DataFrame):

    s_Year = pdf_CatMain['Year'].dropna().astype(str).unique()
    s_Year = sorted([year for year in s_Year if year.isdigit()], reverse=True)

    return html.Div([
        html.Div(
            className="card-header",
            children=[
                html.H6(title, className="graph-title"),
                dcc.Dropdown(
                    id='dropdown-Year',
                    className="dropdown-year",
                    options=(
                        [{'label': 'All', 'value': 'All'}] +
                        [{'label': str(year), 'value': str(year)} for year in s_Year]
                    ),
                    value='All',
                    clearable=False
                ),
            ]
        ),
        dcc.Graph(id='fig-Donut')
    ], className='graph-container graph-card-enhanced half-width-card')


def make_table_card(title: str, s_col: list, data: dict, table_id: str):
    """make table card

    Args:
        title (str): title for the Card
        s_col (list): columns to show from the table
        data (dict): data
        table_id (str): table id
    """

    return html.Div([
        html.H6(title, className="graph-title"),
        dash_table.DataTable(
            id=table_id,
            columns=s_col,
            data=data,
            style_table={'overflowX': 'auto'}
        )
    ], className="graph-container full-width-card")


def format_diff(pct: float):
    sign = '+' if pct >= 0 else '-'
    color = 'kpi-red' if pct >= 0 else 'kpi-green'
    return f'{sign}{abs(pct):.1f} %', color



def make_TopCategory_card(
    title: str,
    Category: str,
    MonthLast: str,
    amount_MonthLast: float,
    amount_MonthPrev: float,
    amount_12m_avg: float,
    diff_prev_pct: float,
    diff_12m_pct: float
):
    text_prev, class_prev = format_diff(diff_prev_pct)
    text_12m, class_12m = format_diff(diff_12m_pct)

    return html.Div([
        html.H6(f'{title} ({MonthLast})', className="card-title"),

        # Headline: Category · Amount
        html.Div(f"{Category} · {amount_MonthLast:,.0f} CHF", className="kpi-headline"),

        # Difference vs previous month
        html.Div([html.Span(text_prev, className=f"kpi-diff {class_prev}")], className="kpi-diff-row"),
        html.Div(f"prev: {amount_MonthPrev:,.0f} CHF", className="kpi-subtext"),

        # Difference vs 12-month average
        html.Div([html.Span(text_12m, className=f"kpi-diff {class_12m}")], className="kpi-diff-row"),
        html.Div(f"12m avg: {amount_12m_avg:,.0f} CHF", className="kpi-subtext"),
    ], className="graph-container graph-card-enhanced kpi-card")