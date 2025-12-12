import dash
from dash import html, dcc, Input, Output, dash_table, ctx
import pandas as pd
from app.config import FDP, VIS
from app.vis.Figure import Fig
import plotly.graph_objects as go
fdp = FDP()
vis = VIS()

F = Fig()

# ----- load Data -----
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)
pdf_Balance = pd.read_pickle(fdp.pth_table_Balance)
pdf_Grocery = pd.read_pickle(fdp.pth_table_Groceries)
pdf_Food = pd.read_pickle(fdp.pth_table_Food)

z_StatsTable = pd.read_pickle(fdp.pth_table_Stats)
pdf_TopExpenses = pd.read_pickle(fdp.pth_table_TopExpenses)
pdf_TopExpenses['Date'] = pdf_TopExpenses['Date'].dt.strftime('%d-%m-%Y')
scol_TopExpenses = [{'name': col, 'id': col, **vis.vk_format_col.get(col, {'type': 'text'})} for col in pdf_TopExpenses.columns]

Balance = pdf.sort_values(by='Date', ascending=False)['Balance_CHF'].iloc[0]

# ----- functions -----
def get_balance_class(value):
    return "card-value balance-positive" if value > 0 else "card-value balance-negative"


def make_figure_card(title: str, fig: go.Figure):
    return html.Div([
        html.H6(title, className="graph-title"),
        dcc.Graph(figure=fig)
    ], className="graph-container graph-card-enhanced")


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
    ], className="graph-container")



# ---------- Initialize App ----------
app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server

# ---------- Layout ----------
app.layout = html.Div([
    # Sidebar
    html.Div([
        html.H2("Expense Tracker", className="logo"),
        html.Div([
            dcc.Link("🏠 Home", href="/", id="link-home", className="menu-link"),
            dcc.Link("🛒 Groceries", href="/groceries", id="link-groceries", className="menu-link"),
            dcc.Link("🍽️ Dining & Bars", href="/food", id="link-food", className="menu-link"),
            dcc.Link("✈️ Transport Analytics", href="/transport", id="link-transport", className="menu-link"),
            dcc.Link("⛳ Sport Analytics", href="/sport", id="link-sport", className="menu-link"),
        ], className="menu"),
    ], className="sidebar"),

    dcc.Location(id="url", refresh=False),

    # Page Content
    html.Div(id="page-content", className="content")
])

# ---------- Home Page ----------
home_layout = html.Div([
    html.Div([
        html.Div([
            html.H6("Current Balance", className="card-title"),
            html.P(f"{z_StatsTable['Balance_current']:,.2f} CHF", className="card-value balance")
        ], className="card"),

        html.Div([
            html.H6("Average net Balance (3 months)", className="card-title"),
            html.P(
                f"{z_StatsTable['Balance_net_3months']:,.2f} CHF",
                className=get_balance_class(z_StatsTable['Balance_net_3months']),
            )
        ], className="card"),

        html.Div([
            html.H6("Average net Balance (12 months)", className="card-title"),
            html.P(
                f"{z_StatsTable['Balance_net_12months']:,.2f} CHF",
                className=get_balance_class(z_StatsTable['Balance_net_12months']),
            )
        ], className="card"),
    ], className="cards-container"),

    
    make_figure_card("Balance Progression", F.fig_BalancePerDay(pdf_Balance)),
    make_table_card(
        title='Top 20 Expenses',
        s_col=scol_TopExpenses,
        data=pdf_TopExpenses.to_dict('records'),
        table_id="table-top-expenses"
    ),
    
])


# --------- Grocery Page ----------
groceries_layout = html.Div([
    html.H2("🛒 Grocery store Analytics", className="page-title-center"),
    make_double_figure_card_MonthYear('Grocery store expenses [CHF / %]', 'fig-Abs', 'fig-Pct'),
    make_figure_card('Grocery store expenses per visit [CHF]', F.fig_BoxGrocery(pdf))
])

# ---------- Food Page ---------
food_layout = html.Div([
    html.H2("🍽️ Dining Analytics", className="page-title-center"),
    make_figure_card_MonthYear('Food & Dining expenses [CHF]', 'fig-Food'),
    make_figure_card('Food & Dining expenses per visit [CHF]', F.fig_BoxFood(pdf))
])

# ---------- Callbacks ----------
@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)
def display_page(pth):
    if pth == "/":
        return home_layout
    elif pth == '/groceries':
        return groceries_layout
    elif pth == '/food':
        return food_layout
    

@app.callback(
    [
        Output("link-home", "className"),
        Output("link-groceries", "className"),
        Output("link-food", "className"),
        Output("link-transport", "className"),
        Output("link-sport", "className")
    ],
    Input("url", "pathname")
)
def highlight_active_tab(pth):
    default = "menu-link"
    active = "menu-link active"
    return [
        active if pth == "/" else default,
        active if pth == "/groceries" else default,
        active if pth == "/food" else default,
        active if pth == "/transport" else default,
        active if pth == "/sport" else default,
    ]


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
        monthly_class = "freq-btn"
        yearly_class = "freq-btn freq-btn-active"
    else:
        freq = "Monthly"
        monthly_class = "freq-btn freq-btn-active"
        yearly_class = "freq-btn"

    fig_abs = F.fig_BarGrocery(pdf_Grocery, freq)
    fig_pct = F.fig_BarGrocery_pct(pdf_Grocery, freq)

    return fig_abs, fig_pct, monthly_class, yearly_class


@app.callback(
    Output("fig-Food", "figure"),
    Output("fig-Food-monthly", "className"),
    Output("fig-Food-yearly", "className"),
    Input("fig-Food-monthly", "n_clicks"),
    Input("fig-Food-yearly", "n_clicks"),
)
def update_FoodPlot(n_monthly, n_yearly):

    trigger = ctx.triggered_id

    if trigger == "fig-Food-yearly":
        freq = "Yearly"
        monthly_class = "freq-btn"
        yearly_class = "freq-btn freq-btn-active"
    else:
        freq = "Monthly"
        monthly_class = "freq-btn freq-btn-active"
        yearly_class = "freq-btn"

    fig = F.fig_BarFood(pdf_Food, freq)

    return fig, monthly_class, yearly_class



if __name__ == "__main__":
    app.run(debug=True)
