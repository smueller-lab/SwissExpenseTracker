import dash
from dash import html, dcc, Input, Output, dash_table
import plotly.express as px
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


# ----- load figures -----
with open(fdp.pth_fig_BalancePerDay) as f:
    fig_balance = from_json(f.read())


# ----- functions -----
def get_balance_class(value):
    return "card-value balance-positive" if value > 0 else "card-value balance-negative"

# ---------- Initialize App ----------
app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server

# ---------- Layout ----------
app.layout = html.Div([
    # Sidebar
    html.Div([
        html.H2("Expense Tracker", className="logo"),
        html.Div([
            dcc.Link("🏠 Home", href="/", className="menu-link active"),
            dcc.Link("🍽️ Food Analytics", href="/food", className="menu-link"),
            dcc.Link("✈️ Transport Analytics", href="/transport", className="menu-link"),
            dcc.Link("⛳ Sport Analytics", href="/sport", className="menu-link"),
        ], className="menu"),
    ], className="sidebar"),

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

    html.Div([
        html.Div([
            html.H3('Balance Progression', className='graph-title'),
            dcc.Graph(figure=fig_balance)
        ], className="graph-container"),

        html.Div([
            html.H3('Top 20 Expenses', className='graph-title'),
            dash_table.DataTable(
                id='table-top-expenses',
                columns=scol_TopExpenses,
                data=pdf_TopExpenses.to_dict('records'),
                style_table={'overflowX': 'auto'},
            )
        ], className='graph-container')
    ])
])

# Placeholder pages
analytics_layout = html.Div([
    html.H2("📊 Analytics page - coming soon!", style={"color": "white"})
])

settings_layout = html.Div([
    html.H2("⚙️ Settings page - coming soon!", style={"color": "white"})
])

# ---------- Callbacks ----------
@app.callback(Output("page-content", "children"),
              [Input("page-content", "id")])
def display_page(_):
    return home_layout  # Only the Home page for now


if __name__ == "__main__":
    app.run(debug=True)
