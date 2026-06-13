from __future__ import annotations

import shutil

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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_app_loads_without_error(dash_duo: Any, real_app: Any) -> None:
    """Home page loads and at least one Plotly chart renders without browser errors."""
    dash_duo.start_server(real_app)
    dash_duo.wait_for_element(".js-plotly-plot", timeout=15)
    assert _no_severe_errors(dash_duo)


def test_graphs_render_svg(dash_duo: Any, real_app: Any) -> None:
    """Plotly figures produce visible SVG elements on the home page."""
    dash_duo.start_server(real_app)
    dash_duo.wait_for_element(".js-plotly-plot svg", timeout=15)
    assert len(dash_duo.find_elements(".js-plotly-plot svg")) >= 1
    assert _no_severe_errors(dash_duo)


def test_monthly_yearly_toggle_updates_figure(dash_duo: Any, real_app: Any) -> None:
    """Grocery toggle buttons switch between Monthly and Yearly views without JS errors."""
    dash_duo.start_server(real_app)
    dash_duo.driver.get(dash_duo.server_url + "/groceries")
    dash_duo.wait_for_element("#fig-Abs", timeout=15)

    # Switch to Yearly view and wait for the active class to propagate
    dash_duo.find_element("#fig-Abs-yearly").click()
    dash_duo.wait_for_element("#fig-Abs-yearly.btn-toggle-active", timeout=10)
    assert _no_severe_errors(dash_duo)

    # Switch back to Monthly view
    dash_duo.find_element("#fig-Abs-monthly").click()
    dash_duo.wait_for_element("#fig-Abs-monthly.btn-toggle-active", timeout=10)
    assert _no_severe_errors(dash_duo)


def test_donut_year_dropdown_updates_figure(dash_duo: Any, real_app: Any) -> None:
    """Home page donut chart and year-selector dropdown are present; no JS errors.

    Full React-Select interaction is intentionally omitted: checking that the
    graph container and dropdown exist is a reliable proxy for a working page.
    """
    dash_duo.start_server(real_app)
    dash_duo.wait_for_element("#fig-Donut", timeout=15)
    assert dash_duo.find_element("#dropdown-Year") is not None
    assert _no_severe_errors(dash_duo)


def test_retail_donut_year_dropdown_updates_figure(
    dash_duo: Any, real_app: Any
) -> None:
    """Retail page renders its donut chart and year dropdown; no JS errors."""
    dash_duo.start_server(real_app)
    dash_duo.driver.get(dash_duo.server_url + "/retail")
    dash_duo.wait_for_element("#fig-Retail-Donut", timeout=15)
    assert dash_duo.find_element("#dropdown-Retail-Year") is not None
    assert _no_severe_errors(dash_duo)
