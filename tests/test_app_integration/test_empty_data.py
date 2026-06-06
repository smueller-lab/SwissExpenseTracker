from __future__ import annotations

import shutil
import unittest.mock

from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Skip guard — integration tests require Chrome + dash[testing] extras
# ---------------------------------------------------------------------------

if (
    not shutil.which("chromedriver")
    and not shutil.which("chromium-browser")
    and not shutil.which("google-chrome")
):
    pytest.skip(
        "chromedriver not found — install chromium-driver to run integration tests",
        allow_module_level=True,
    )

try:
    from dash.testing.application_runners import import_app as _  # noqa: F401
except ImportError:
    pytest.skip(
        "dash[testing] not installed — run: pip install 'dash[testing]'",
        allow_module_level=True,
    )

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _no_severe_errors(dash_duo: Any) -> bool:
    """Return True when there are no SEVERE-level browser console errors."""
    return all(log.get("level") != "SEVERE" for log in dash_duo.get_logs())


def _make_app(data: Any, pos: Any) -> Any:
    """Create a fresh Dash app wired to the given data loaders.

    Patches run_dashboard_pipeline to suppress the module-level side effect
    in app.py when it is first imported.
    """
    with unittest.mock.patch(
        "swiss_exp_tracker.pipeline_dash.pipeline.run_dashboard_pipeline"
    ):
        from swiss_exp_tracker.app.app import create_app_factory

    return create_app_factory(data, pos)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_investing_page_no_crash_with_empty_positions(
    dash_duo: Any, real_data: Any, empty_pos: Any
) -> None:
    """Investing page renders without a crash when the positions DB does not exist."""
    app = _make_app(real_data, empty_pos)
    dash_duo.start_server(app)
    dash_duo.driver.get(dash_duo.server_url + "/investing")
    dash_duo.wait_for_element("#page-content", timeout=15)

    # No Dash error overlay should be visible
    error_banners = dash_duo.find_elements(".dash-error-card")
    assert len(error_banners) == 0

    # "No Swissquote data" appears either as Plotly annotation or as a <p> from the
    # update_performers callback (both render visible text captured by body.text)
    page_text = dash_duo.driver.find_element("tag name", "body").text
    assert "No Swissquote data" in page_text

    assert _no_severe_errors(dash_duo)


def test_investing_performers_shows_message_not_crash(
    dash_duo: Any, real_data: Any, empty_pos: Any
) -> None:
    """Performers panel shows the no-data message when the positions DB is absent."""
    app = _make_app(real_data, empty_pos)
    dash_duo.start_server(app)
    dash_duo.driver.get(dash_duo.server_url + "/investing")
    dash_duo.wait_for_element("#investing-performers-content", timeout=15)

    performers_text = dash_duo.find_element("#investing-performers-content").text
    assert "No Swissquote data" in performers_text
    assert _no_severe_errors(dash_duo)


def test_mcumulus_page_no_crash_with_empty_cumulus_data(
    dash_duo: Any, empty_cumulus_data: Any, real_pos: Any
) -> None:
    """M-Cumulus page renders without a crash when grocery item data is empty.

    The figure functions return _make_no_data_fig('No Migros Cumulus data...')
    annotations which Plotly renders as visible SVG text captured by body.text.
    """
    app = _make_app(empty_cumulus_data, real_pos)
    dash_duo.start_server(app)
    dash_duo.driver.get(dash_duo.server_url + "/m-cumulus")
    dash_duo.wait_for_element("#page-content", timeout=15)

    # No Dash error overlay
    error_banners = dash_duo.find_elements(".dash-error-card")
    assert len(error_banners) == 0

    page_text = dash_duo.driver.find_element("tag name", "body").text
    assert "No Migros Cumulus data" in page_text
    assert _no_severe_errors(dash_duo)
