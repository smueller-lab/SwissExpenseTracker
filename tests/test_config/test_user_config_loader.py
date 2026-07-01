"""Tests for load_credit_card_payment_texts in user_config_loader.py."""

from __future__ import annotations

from pathlib import Path

import yaml

from swiss_exp_tracker.config.user_config_loader import load_credit_card_payment_texts


def test_load_credit_card_payment_texts_happy_path(tmp_path: Path) -> None:
    """File with credit_card_payments section returns the booking_texts list."""
    detected_path = tmp_path / "detected_rules.yaml"
    detected_path.write_text(
        yaml.safe_dump(
            {"credit_card_payments": {"booking_texts": ["Ihre Zahlung - Danke"]}}
        ),
        encoding="utf-8",
    )

    result = load_credit_card_payment_texts(detected_path)

    assert result == ["Ihre Zahlung - Danke"]


def test_load_credit_card_payment_texts_multiple_entries(tmp_path: Path) -> None:
    """File with multiple booking_texts entries returns all of them."""
    detected_path = tmp_path / "detected_rules.yaml"
    detected_path.write_text(
        yaml.safe_dump(
            {
                "credit_card_payments": {
                    "booking_texts": [
                        "Ihre Zahlung - Danke",
                        "2002 LSV-Zahlung",
                        "UBS Card Center",
                    ]
                }
            }
        ),
        encoding="utf-8",
    )

    result = load_credit_card_payment_texts(detected_path)

    assert result == ["Ihre Zahlung - Danke", "2002 LSV-Zahlung", "UBS Card Center"]


def test_load_credit_card_payment_texts_missing_file_returns_empty(
    tmp_path: Path,
) -> None:
    """Absent detected_rules.yaml returns an empty list without raising."""
    result = load_credit_card_payment_texts(tmp_path / "does_not_exist.yaml")

    assert result == []


def test_load_credit_card_payment_texts_no_credit_card_section_returns_empty(
    tmp_path: Path,
) -> None:
    """File with only salary/housing sections (no credit_card_payments key) returns []."""
    detected_path = tmp_path / "detected_rules.yaml"
    detected_path.write_text(
        yaml.safe_dump(
            {
                "salary": {"employers": ["My Corp"]},
                "housing": {"rent": [{"merchant": "Landlord AG", "amounts": [1200.0]}]},
            }
        ),
        encoding="utf-8",
    )

    result = load_credit_card_payment_texts(detected_path)

    assert result == []
