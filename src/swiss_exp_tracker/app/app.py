from __future__ import annotations

import dash

from swiss_exp_tracker.app.callbacks import register_all_callbacks
from swiss_exp_tracker.app.data.loader import DataLoader
from swiss_exp_tracker.app.data.loader_positions import PositionsLoader
from swiss_exp_tracker.app.layout.app import create_app_layout
from swiss_exp_tracker.pipeline_dash.pipeline import run_dashboard_pipeline


app = dash.Dash(__name__, suppress_callback_exceptions=True, title="Expense Tracker")

server = app.server

# ----- Rebuild dash_* tables from latest transactions_use -----
run_dashboard_pipeline()

# ----- Load data -----
data = DataLoader()
pos_data = PositionsLoader()


# ----- Set layout -----
app.layout = create_app_layout()


# ----- register callbacks -----
register_all_callbacks(app, data, pos_data)


# ----- Run app -----
if __name__ == "__main__":
    app.run(debug=True)
