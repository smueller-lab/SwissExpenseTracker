from __future__ import annotations

import dash

from app.callbacks import register_all_callbacks
from app.data.loader import DataLoader
from app.layout.app import create_app_layout


app = dash.Dash(__name__, suppress_callback_exceptions=True, title="Expense Tracker")

server = app.server

# ----- Load data -----
data = DataLoader()


# ----- Set layout -----
app.layout = create_app_layout()


# ----- register callbacks -----
register_all_callbacks(app, data)


# ----- Run app -----
if __name__ == "__main__":
    app.run(debug=True)
