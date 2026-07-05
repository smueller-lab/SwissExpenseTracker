"""Tests for the app.py helpers _data_through_label and _app_version.

app.py performs heavy work at import time, so every test depends on the app_module
fixture (see conftest) which imports it with those side effects stubbed. The helper
functions are imported directly so they carry their real type signatures.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from types import SimpleNamespace
from typing import cast

import pandas as pd
import pytest

from pytest_mock import MockerFixture

from swiss_exp_tracker.app.data.loader import DataLoader


def _make_loader(dates: list[str | None]) -> DataLoader:
    """Return a stub loader whose pdf_Master has a 'date' column from the given values."""
    series = pd.to_datetime(pd.Series(dates, dtype="object"))
    stub = SimpleNamespace(pdf_Master=pd.DataFrame({"date": series}))
    return cast("DataLoader", stub)


# --- _data_through_label ---------------------------------------------------


@pytest.mark.usefixtures("app_module")
def test_data_through_label_returns_latest_month() -> None:
    """The most recent transaction date is formatted as 'Mon YYYY'."""
    from swiss_exp_tracker.app.app import _data_through_label

    loader = _make_loader(["2026-04-15", "2026-06-03", "2026-05-20"])
    assert _data_through_label(loader) == "Jun 2026"


@pytest.mark.usefixtures("app_module")
def test_data_through_label_empty_returns_dash() -> None:
    """An empty pdf_Master yields the em-dash placeholder."""
    from swiss_exp_tracker.app.app import _data_through_label

    assert _data_through_label(_make_loader([])) == "—"


@pytest.mark.usefixtures("app_module")
def test_data_through_label_all_nat_returns_dash() -> None:
    """A date column that is entirely NaT yields the em-dash placeholder."""
    from swiss_exp_tracker.app.app import _data_through_label

    assert _data_through_label(_make_loader([None, None])) == "—"


# --- _app_version ----------------------------------------------------------


@pytest.mark.usefixtures("app_module")
def test_app_version_returns_installed_version(mocker: MockerFixture) -> None:
    """The installed package version is returned verbatim."""
    from swiss_exp_tracker.app.app import _app_version

    mocker.patch("swiss_exp_tracker.app.app.pkg_version", return_value="9.9.9")
    assert _app_version() == "9.9.9"


@pytest.mark.usefixtures("app_module")
def test_app_version_missing_metadata_returns_empty(mocker: MockerFixture) -> None:
    """A missing package falls back to an empty string."""
    from swiss_exp_tracker.app.app import _app_version

    mocker.patch(
        "swiss_exp_tracker.app.app.pkg_version",
        side_effect=PackageNotFoundError("swiss_exp_tracker"),
    )
    assert _app_version() == ""
