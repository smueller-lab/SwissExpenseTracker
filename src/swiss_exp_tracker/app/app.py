from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version

import dash
import pandas as pd

from swiss_exp_tracker.app.callbacks import register_all_callbacks
from swiss_exp_tracker.app.data.loader import DataLoader
from swiss_exp_tracker.app.data.loader_positions import PositionsLoader
from swiss_exp_tracker.app.layout.app import create_app_layout
from swiss_exp_tracker.pipeline_dash.pipeline import run_dashboard_pipeline


def _data_through_label(data_loader: DataLoader) -> str:
    """Return the most recent transaction month as 'Mon YYYY', or '—' if no data."""
    latest = data_loader.pdf_Master["date"].max()
    return "—" if pd.isna(latest) else latest.strftime("%b %Y")


def _app_version() -> str:
    """Return the installed package version, or '' if metadata is unavailable."""
    try:
        return pkg_version("swiss_exp_tracker")
    except PackageNotFoundError:
        return ""


def create_app_factory(
    data_loader: DataLoader, pos_loader: PositionsLoader
) -> dash.Dash:
    """Return a configured Dash app instance wired to the given data loaders."""
    _app = dash.Dash(
        __name__, suppress_callback_exceptions=True, title="Expense Tracker"
    )
    _app.layout = create_app_layout(
        data_through=_data_through_label(data_loader),
        n_transactions=len(data_loader.pdf_Master),
        app_version=_app_version(),
    )
    register_all_callbacks(_app, data_loader, pos_loader)
    return _app


# ----- Rebuild dash_* tables from latest transactions_use -----
run_dashboard_pipeline()

# ----- Load data -----
data = DataLoader()
pos_data = PositionsLoader()

app = create_app_factory(data, pos_data)
server = app.server


# ----- Run app -----
if __name__ == "__main__":
    app.run(debug=True)
