import dash
from dash import html, dcc, Input, Output, dash_table
from plotly.io import from_json
import pandas as pd
from config import FDP, VIS
fdp = FDP()
vis = VIS()

# ----- load Data -----
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)

z_StatsTable = pd.read_pickle(fdp.pth_table_Stats)
pdf_TopExpenses = pd.read_pickle(fdp.pth_table_TopExpenses)
pdf_TopExpenses['Date'] = pdf_TopExpenses['Date'].dt.strftime('%d-%m-%Y')
scol_TopExpenses = [{'name': col, 'id': col, **vis.vk_format_col.get(col, {'type': 'text'})} for col in pdf_TopExpenses.columns]

Balance = pdf.sort_values(by='Date', ascending=False)['Balance_CHF'].iloc[0]

# ----- functions -----
def get_balance_class(value):
    return "card-value balance-positive" if value > 0 else "card-value balance-negative"


def make_figure_card(title: str, pth_fig: str):
    with open(pth_fig, "r") as f:
        fig = from_json(f.read())

    return html.Div([
        html.H6(title, className="graph-title"),
        dcc.Graph(figure=fig)
    ], className="graph-container")


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
            dcc.Link("🍽️ Restaurant & Bars", href="/restaurant", id="link-restaurant", className="menu-link"),
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

    
    make_figure_card("Balance Progression", fdp.pth_fig_BalancePerDay),
    make_table_card(
        title='Top 20 Expenses',
        s_col=scol_TopExpenses,
        data=pdf_TopExpenses.to_dict('records'),
        table_id="table-top-expenses"
    ),
    
])


# --------- Grocery Page ----------
groceries_layout = html.Div([
    html.H2("🛒 Groceries Analytics", className="page-title-center"),
    make_figure_card("Grocery Expenses per month", fdp.pth_fig_GroceryBar),
    make_figure_card("Grocery Expense Merchant distribution per month [%]", fdp.pth_fig_GroceryBarPct),
    make_figure_card("Total Grocery Expenses vs. fraction of Merchant from total expenses", fdp.pth_fig_GroceryLR)
])

# Placeholder pages
analytics_layout = html.Div([
    html.H2("📊 Analytics page - coming soon!", style={"color": "white"})
])

settings_layout = html.Div([
    html.H2("⚙️ Settings page - coming soon!", style={"color": "white"})
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
    

@app.callback(
    [
        Output("link-home", "className"),
        Output("link-groceries", "className"),
        Output("link-restaurant", "className"),
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
        active if pth == "/restaurant" else default,
        active if pth == "/transport" else default,
        active if pth == "/sport" else default,
    ]



if __name__ == "__main__":
    app.run(debug=True)
