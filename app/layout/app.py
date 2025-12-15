from dash import html, dcc

def create_app_layout():
    return html.Div([

        # ---------------- Sidebar ----------------
        html.Div([
            html.H2("Expense Tracker", className="logo"),

            html.Div([
                dcc.Link("🏠 Home", href="/", id="link-home", className="menu-link"),
                dcc.Link("🛒 Groceries", href="/groceries", id="link-groceries", className="menu-link"),
                dcc.Link("🍽️ Dining & Bars", href="/food", id="link-food", className="menu-link"),
                dcc.Link("✈️ Transport", href="/transport", id="link-transport", className="menu-link"),
                dcc.Link("⛳ Sport", href="/sport", id="link-sport", className="menu-link"),
            ], className="menu"),

        ], className="sidebar"),

        # ---------------- Router ----------------
        dcc.Location(id="url", refresh=False),

        # ---------------- Page content ----------------
        html.Div(
            id="page-content",
            className="content"
        )

    ])
