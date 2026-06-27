"""Shared fixtures for test_app.

app.py runs the dashboard pipeline and builds the loaders + Dash app at import
time. The app_module fixture stubs those side effects so importing the module is
cheap and database-free; tests then import its helper functions directly.
"""

from __future__ import annotations

import importlib
import sys

from collections.abc import Iterator
from types import ModuleType

import pytest

from dash import html
from pytest_mock import MockerFixture


@pytest.fixture
def app_module(mocker: MockerFixture) -> Iterator[ModuleType]:
    """Import swiss_exp_tracker.app.app with all import-time side effects stubbed."""
    mocker.patch("swiss_exp_tracker.pipeline_dash.pipeline.run_dashboard_pipeline")
    mocker.patch("swiss_exp_tracker.app.data.loader.DataLoader")
    mocker.patch("swiss_exp_tracker.app.data.loader_positions.PositionsLoader")
    mocker.patch("swiss_exp_tracker.app.callbacks.register_all_callbacks")
    mocker.patch(
        "swiss_exp_tracker.app.layout.app.create_app_layout",
        return_value=html.Div(),
    )

    sys.modules.pop("swiss_exp_tracker.app.app", None)
    module = importlib.import_module("swiss_exp_tracker.app.app")
    try:
        yield module
    finally:
        sys.modules.pop("swiss_exp_tracker.app.app", None)
