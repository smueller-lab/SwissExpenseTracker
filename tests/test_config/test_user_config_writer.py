"""Tests for user_config_writer.py's comment-preserving YAML upserts."""

from __future__ import annotations

from pathlib import Path

import pytest

from swiss_exp_tracker.config.user_config_loader import load
from swiss_exp_tracker.config.user_config_schema import CustomRule
from swiss_exp_tracker.config.user_config_schema import ReferenceIdCorrection
from swiss_exp_tracker.config.user_config_writer import upsert_custom_rule
from swiss_exp_tracker.config.user_config_writer import upsert_reference_id_correction

_FIXTURE_YAML = """\
salary:
  employers:
    - "OYM AG"
  employers_exclude: []
  donations: []

housing:
  rent: []
  rent_exclude: []
  deposits: []
  shared_housing: []

# Rename a merchant's display name without changing its category.
# match:       text compared (case-insensitive) against the enriched merchant name
merchant_renames:
  - match: "some merchant"
    rename_to: "Clean Name"

custom_rules:
  - merchant: "revolut"
    category_main: "Payment Services"
    category_second: "Money Transfer"
    exact_match: true
  # Personal transfer to my own broker.
  - merchant: "sebastian muller"
    category_main: "Investing"
    category_second: "Brokerage"
    exact_match: true

reference_id_corrections:
  - reference_id: "NOID-existing-1"
    merchant: "Existing Merchant"
    category_main: "Existing"
    category_second: "Category"
    city: "Zurich"
"""


def _make_rule(**overrides: object) -> CustomRule:
    base: dict[str, object] = {
        "merchant": "Amazon",
        "category_main": "Retail",
        "category_second": "Shopping",
        "exact_match": True,
    }
    base.update(overrides)
    return CustomRule.model_validate(base)


def _make_correction(**overrides: object) -> ReferenceIdCorrection:
    base: dict[str, object] = {
        "reference_id": "NOID-new-1",
        "merchant": "New Merchant",
        "category_main": "New",
        "category_second": "Category",
        "city": None,
    }
    base.update(overrides)
    return ReferenceIdCorrection.model_validate(base)


def _write_fixture(path: Path) -> None:
    path.write_text(_FIXTURE_YAML, encoding="utf-8")


def test_upsert_custom_rule_appends_new_merchant(tmp_path: Path) -> None:
    """A merchant with no existing exact-match rule is appended to custom_rules."""
    path = tmp_path / "user_config.yaml"
    _write_fixture(path)

    upsert_custom_rule(_make_rule(merchant="Amazon"), path=path)

    cfg = load(user_config_path=path)
    amazon_rules = [r for r in cfg.custom_rules if r.merchant == "Amazon"]
    assert len(amazon_rules) == 1
    assert amazon_rules[0].category_main == "Retail"
    assert len(cfg.custom_rules) == 3


def test_upsert_custom_rule_replaces_existing_merchant(tmp_path: Path) -> None:
    """Re-editing the same merchant replaces the existing exact-match rule, not duplicates it."""
    path = tmp_path / "user_config.yaml"
    _write_fixture(path)

    upsert_custom_rule(
        _make_rule(
            merchant="revolut", category_main="Payment Services", category_second="Fees"
        ),
        path=path,
    )

    cfg = load(user_config_path=path)
    revolut_rules = [r for r in cfg.custom_rules if r.merchant.lower() == "revolut"]
    assert len(revolut_rules) == 1
    assert revolut_rules[0].category_second == "Fees"
    assert len(cfg.custom_rules) == 2


def test_upsert_custom_rule_preserves_comments_and_unrelated_sections(
    tmp_path: Path,
) -> None:
    """Writing a custom_rules entry leaves comments and other sections untouched."""
    path = tmp_path / "user_config.yaml"
    _write_fixture(path)

    upsert_custom_rule(_make_rule(merchant="Amazon"), path=path)

    result = path.read_text(encoding="utf-8")
    assert "# Rename a merchant's display name without changing its category." in result
    assert "# Personal transfer to my own broker." in result
    assert '- "OYM AG"' in result
    assert '- match: "some merchant"' in result
    assert '    rename_to: "Clean Name"' in result


def test_upsert_reference_id_correction_appends_new_reference(tmp_path: Path) -> None:
    """A reference_id with no existing entry is appended to reference_id_corrections."""
    path = tmp_path / "user_config.yaml"
    _write_fixture(path)

    upsert_reference_id_correction(
        _make_correction(reference_id="NOID-new-1"), path=path
    )

    cfg = load(user_config_path=path)
    matches = [
        c for c in cfg.reference_id_corrections if c.reference_id == "NOID-new-1"
    ]
    assert len(matches) == 1
    assert len(cfg.reference_id_corrections) == 2


def test_upsert_reference_id_correction_replaces_existing_reference(
    tmp_path: Path,
) -> None:
    """Re-editing the same reference_id replaces the existing entry, not duplicates it."""
    path = tmp_path / "user_config.yaml"
    _write_fixture(path)

    upsert_reference_id_correction(
        _make_correction(reference_id="NOID-existing-1", merchant="Renamed Merchant"),
        path=path,
    )

    cfg = load(user_config_path=path)
    matches = [
        c for c in cfg.reference_id_corrections if c.reference_id == "NOID-existing-1"
    ]
    assert len(matches) == 1
    assert matches[0].merchant == "Renamed Merchant"
    assert len(cfg.reference_id_corrections) == 1


def test_upsert_reference_id_correction_omits_city_when_none(tmp_path: Path) -> None:
    """A correction with city=None does not write a null city key into the YAML."""
    path = tmp_path / "user_config.yaml"
    _write_fixture(path)

    upsert_reference_id_correction(
        _make_correction(reference_id="NOID-no-city", city=None), path=path
    )

    result = path.read_text(encoding="utf-8")
    lines = [
        line
        for line in result.splitlines()
        if "NOID-no-city" in line
        or (line.strip().startswith("city:") and "Zurich" not in line)
    ]
    assert not any("city:" in line for line in lines)


def test_upsert_custom_rule_rejects_invalid_input() -> None:
    """Constructing a CustomRule with a missing required field raises before any write happens."""
    with pytest.raises(ValueError):
        CustomRule.model_validate({"merchant": "Amazon", "category_main": "Retail"})


def test_upsert_reference_id_correction_rejects_invalid_input() -> None:
    """Constructing a ReferenceIdCorrection with a missing required field raises before any write happens."""
    with pytest.raises(ValueError):
        ReferenceIdCorrection.model_validate(
            {"reference_id": "NOID-1", "merchant": "X"}
        )
