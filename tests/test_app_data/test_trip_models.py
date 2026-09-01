"""Tests for swiss_exp_tracker.app.data.trip_models.Trip."""

from __future__ import annotations

from datetime import datetime

import pytest

from pydantic import ValidationError

from swiss_exp_tracker.app.data.trip_models import Trip

# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def _make_trip_payload(**overrides: object) -> dict[str, object]:
    """Return a valid Trip payload; each test states only what it varies."""
    base: dict[str, object] = {
        "name": "Ibiza Summer",
        "year": 2026,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_trip_model_happy_path_fields_populated() -> None:
    """model_validate with valid inputs populates both fields correctly."""
    m = Trip.model_validate(_make_trip_payload())
    assert m.name == "Ibiza Summer"
    assert m.year == 2026


def test_trip_model_minimum_valid_year() -> None:
    """Year 1990 (the lower bound) is accepted."""
    m = Trip.model_validate(_make_trip_payload(year=1990))
    assert m.year == 1990


def test_trip_model_maximum_valid_year() -> None:
    """Year current_year+5 (the upper bound) is accepted without raising."""
    upper = datetime.now().year + 5
    m = Trip.model_validate(_make_trip_payload(year=upper))
    assert m.year == upper


def test_trip_model_current_year_is_valid() -> None:
    """The current calendar year is within the accepted range."""
    current = datetime.now().year
    m = Trip.model_validate(_make_trip_payload(year=current))
    assert m.year == current


# ---------------------------------------------------------------------------
# Name validation
# ---------------------------------------------------------------------------


def test_trip_model_empty_name_raises_validation_error() -> None:
    """Empty string name raises ValidationError."""
    with pytest.raises(ValidationError):
        Trip.model_validate(_make_trip_payload(name=""))


def test_trip_model_whitespace_only_name_raises_validation_error() -> None:
    """Whitespace-only name raises ValidationError."""
    with pytest.raises(ValidationError):
        Trip.model_validate(_make_trip_payload(name="   "))


def test_trip_model_tab_only_name_raises_validation_error() -> None:
    """A name consisting of only tabs raises ValidationError."""
    with pytest.raises(ValidationError):
        Trip.model_validate(_make_trip_payload(name="\t\t"))


def test_trip_model_leading_trailing_whitespace_stripped() -> None:
    """Leading and trailing whitespace is stripped from name."""
    m = Trip.model_validate(_make_trip_payload(name="  Paris Trip  "))
    assert m.name == "Paris Trip"


def test_trip_model_internal_whitespace_preserved() -> None:
    """Whitespace within a name (not at boundaries) is preserved."""
    m = Trip.model_validate(_make_trip_payload(name="New York 2025"))
    assert m.name == "New York 2025"


def test_trip_model_name_single_character_valid() -> None:
    """A single non-whitespace character is a valid name."""
    m = Trip.model_validate(_make_trip_payload(name="X"))
    assert m.name == "X"


# ---------------------------------------------------------------------------
# Year validation
# ---------------------------------------------------------------------------


def test_trip_model_year_below_lower_bound_raises_validation_error() -> None:
    """Year 1800 (below 1990) raises ValidationError."""
    with pytest.raises(ValidationError):
        Trip.model_validate(_make_trip_payload(year=1800))


def test_trip_model_year_1989_raises_validation_error() -> None:
    """Year 1989 (one below the lower bound 1990) raises ValidationError."""
    with pytest.raises(ValidationError):
        Trip.model_validate(_make_trip_payload(year=1989))


def test_trip_model_year_above_upper_bound_raises_validation_error() -> None:
    """Year current_year+50 (far above upper bound) raises ValidationError."""
    too_far = datetime.now().year + 50
    with pytest.raises(ValidationError):
        Trip.model_validate(_make_trip_payload(year=too_far))


def test_trip_model_year_current_plus_six_raises_validation_error() -> None:
    """Year current_year+6 (one above the upper bound current_year+5) raises ValidationError."""
    with pytest.raises(ValidationError):
        Trip.model_validate(_make_trip_payload(year=datetime.now().year + 6))


def test_trip_model_missing_year_raises_validation_error() -> None:
    """Omitting year (required field, no default) raises ValidationError."""
    with pytest.raises(ValidationError):
        Trip.model_validate({"name": "Barcelona"})


def test_trip_model_missing_name_raises_validation_error() -> None:
    """Omitting name (required field, no default) raises ValidationError."""
    with pytest.raises(ValidationError):
        Trip.model_validate({"year": 2025})


def test_trip_model_year_as_string_is_coerced() -> None:
    """Year supplied as a numeric string is coerced to int without raising."""
    m = Trip.model_validate(_make_trip_payload(year="2025"))
    assert m.year == 2025


# ---------------------------------------------------------------------------
# model_dump() roundtrip
# ---------------------------------------------------------------------------


def test_trip_model_dump_roundtrip_preserves_fields() -> None:
    """model_dump() + model_validate() roundtrip preserves both fields."""
    original = Trip.model_validate(_make_trip_payload())
    dumped = original.model_dump()
    restored = Trip.model_validate(dumped)
    assert restored.name == original.name
    assert restored.year == original.year


def test_trip_model_dump_has_expected_keys() -> None:
    """model_dump() returns a dict with exactly name and year keys."""
    m = Trip.model_validate(_make_trip_payload())
    d = m.model_dump()
    assert set(d.keys()) == {"name", "year"}


def test_trip_model_dump_values_match_fields() -> None:
    """model_dump() dict values match the model's field values."""
    m = Trip.model_validate(_make_trip_payload(name="Greece Trip", year=2025))
    d = m.model_dump()
    assert d["name"] == "Greece Trip"
    assert d["year"] == 2025


def test_trip_model_dump_name_already_stripped() -> None:
    """model_dump() returns the already-stripped name (no surrounding whitespace)."""
    m = Trip.model_validate(_make_trip_payload(name="  Ibiza  "))
    d = m.model_dump()
    assert d["name"] == "Ibiza"
