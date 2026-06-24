"""Tests for UserConfig / DetectedRules schema and the load() merger."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pydantic import ValidationError

from swiss_exp_tracker.config.user_config_loader import load

# ─── helpers ────────────────────────────────────────────────────────────────


def _write_user_config(path: Path, data: dict[str, object]) -> None:
    """Write a user_config.yaml at path."""
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")


def _write_detected_rules(path: Path, data: dict[str, object]) -> None:
    """Write a detected_rules.yaml at path."""
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")


# ─── employer merge tests ────────────────────────────────────────────────────


def test_merge_non_overlapping_employers(tmp_path: Path) -> None:
    """Detected and user have different employers → both appear in merged list."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"
    _write_user_config(user_path, {"salary": {"employers": ["Company B"]}})
    _write_detected_rules(detected_path, {"salary": {"employers": ["Company A"]}})

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    assert "Company A" in cfg.salary.employers
    assert "Company B" in cfg.salary.employers


def test_employers_exclude_removes_detected(tmp_path: Path) -> None:
    """User employers_exclude removes matching detected employers."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"
    _write_user_config(user_path, {"salary": {"employers_exclude": ["A"]}})
    _write_detected_rules(detected_path, {"salary": {"employers": ["A", "B"]}})

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    assert "A" not in cfg.salary.employers
    assert "B" in cfg.salary.employers


def test_employers_exclude_does_not_affect_non_matching(tmp_path: Path) -> None:
    """employers_exclude only removes the named employer, leaves others intact."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"
    _write_user_config(
        user_path, {"salary": {"employers_exclude": ["A"], "employers": ["C"]}}
    )
    _write_detected_rules(detected_path, {"salary": {"employers": ["A", "B"]}})

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    assert "A" not in cfg.salary.employers
    assert "B" in cfg.salary.employers
    assert "C" in cfg.salary.employers


# ─── rent merge tests ────────────────────────────────────────────────────────


def test_user_rent_wins_over_detected(tmp_path: Path) -> None:
    """User rent entry for the same merchant overrides detected amounts."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"
    _write_user_config(
        user_path,
        {"housing": {"rent": [{"merchant": "landlord X", "amounts": [900.0, 1000.0]}]}},
    )
    _write_detected_rules(
        detected_path,
        {"housing": {"rent": [{"merchant": "landlord X", "amounts": [1000.0]}]}},
    )

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    rent_by_merchant = {r.merchant: r for r in cfg.housing.rent}
    assert "landlord X" in rent_by_merchant
    assert rent_by_merchant["landlord X"].amounts == pytest.approx([900.0, 1000.0])


def test_rent_exclude_removes_detected_rent(tmp_path: Path) -> None:
    """User rent_exclude removes the corresponding detected rent entry."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"
    _write_user_config(user_path, {"housing": {"rent_exclude": ["landlord X"]}})
    _write_detected_rules(
        detected_path,
        {"housing": {"rent": [{"merchant": "landlord X", "amounts": [1000.0]}]}},
    )

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    merchants = [r.merchant for r in cfg.housing.rent]
    assert "landlord X" not in merchants


def test_rent_from_detected_preserved_when_not_excluded(tmp_path: Path) -> None:
    """Detected rent entry not in rent_exclude survives the merge."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"
    _write_user_config(user_path, {"housing": {"rent_exclude": ["other"]}})
    _write_detected_rules(
        detected_path,
        {"housing": {"rent": [{"merchant": "kept landlord", "amounts": [1200.0]}]}},
    )

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    merchants = [r.merchant for r in cfg.housing.rent]
    assert "kept landlord" in merchants


# ─── absent-file tests ───────────────────────────────────────────────────────


def test_detected_absent_returns_user_only(tmp_path: Path) -> None:
    """Absent detected_rules.yaml → merged config contains user employers."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"  # does not exist
    _write_user_config(user_path, {"salary": {"employers": ["My Company"]}})

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    assert "My Company" in cfg.salary.employers


def test_user_absent_returns_detected_only(tmp_path: Path) -> None:
    """Absent user_config.yaml → merged config contains detected employers."""
    user_path = tmp_path / "user_config.yaml"  # does not exist
    detected_path = tmp_path / "detected_rules.yaml"
    _write_detected_rules(
        detected_path, {"salary": {"employers": ["Auto Detected Corp"]}}
    )

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    assert "Auto Detected Corp" in cfg.salary.employers


def test_both_absent_returns_empty(tmp_path: Path) -> None:
    """Neither file exists → merged config has all empty lists, no exception."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    assert cfg.salary.employers == []
    assert cfg.housing.rent == []
    assert cfg.investing.transfers == []
    assert cfg.travel.all_inclusive == []
    assert cfg.custom_rules == []


# ─── validation error tests ──────────────────────────────────────────────────


def test_invalid_type_raises(tmp_path: Path) -> None:
    """YAML with non-numeric min_amount for investing transfer raises ValidationError."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"
    _write_user_config(
        user_path,
        {
            "investing": {
                "transfers": [
                    {"merchant": "My Brokerage", "min_amount": "not-a-number"}
                ]
            }
        },
    )

    with pytest.raises(ValidationError):
        load(user_config_path=user_path, detected_path=detected_path)


# ─── pass-through tests ───────────────────────────────────────────────────────


def test_user_only_sections_pass_through(tmp_path: Path) -> None:
    """Deposits, shared_housing, investing and travel from user config appear in merged config."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"
    _write_user_config(
        user_path,
        {
            "housing": {
                "deposits": [{"merchant": "Meine Wohnung AG", "min_amount": 3000.0}],
                "shared_housing": [
                    {"merchant": "Flatmate House", "roommate_offset": 500.0}
                ],
            },
            "investing": {
                "transfers": [{"merchant": "My Brokerage", "min_amount": 1000.0}]
            },
            "travel": {"all_inclusive": [{"merchant": "Sunny Travels", "year": 2024}]},
        },
    )

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    deposit_merchants = [d.merchant for d in cfg.housing.deposits]
    assert "Meine Wohnung AG" in deposit_merchants
    deposit = next(d for d in cfg.housing.deposits if d.merchant == "Meine Wohnung AG")
    assert deposit.min_amount == pytest.approx(3000.0)

    shared_merchants = [s.merchant for s in cfg.housing.shared_housing]
    assert "Flatmate House" in shared_merchants
    shared = next(
        s for s in cfg.housing.shared_housing if s.merchant == "Flatmate House"
    )
    assert shared.roommate_offset == pytest.approx(500.0)

    transfer_merchants = [t.merchant for t in cfg.investing.transfers]
    assert "My Brokerage" in transfer_merchants
    transfer = next(t for t in cfg.investing.transfers if t.merchant == "My Brokerage")
    assert transfer.min_amount == pytest.approx(1000.0)

    travel_merchants = [t.merchant for t in cfg.travel.all_inclusive]
    assert "Sunny Travels" in travel_merchants
    travel = next(t for t in cfg.travel.all_inclusive if t.merchant == "Sunny Travels")
    assert travel.year == 2024


def test_user_custom_rules_pass_through(tmp_path: Path) -> None:
    """Custom rules from user config appear in merged config unchanged."""
    user_path = tmp_path / "user_config.yaml"
    detected_path = tmp_path / "detected_rules.yaml"
    _write_user_config(
        user_path,
        {
            "custom_rules": [
                {
                    "merchant": "My Gym",
                    "category_main": "Sport",
                    "category_second": "Fitness",
                }
            ]
        },
    )

    cfg = load(user_config_path=user_path, detected_path=detected_path)

    rule_merchants = [r.merchant for r in cfg.custom_rules]
    assert "My Gym" in rule_merchants
    rule = next(r for r in cfg.custom_rules if r.merchant == "My Gym")
    assert rule.category_main == "Sport"
    assert rule.category_second == "Fitness"
