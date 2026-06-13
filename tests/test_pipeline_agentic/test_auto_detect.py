"""Tests for salary and rent auto-detection algorithms in auto_detect.py."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from swiss_exp_tracker.config.user_config_schema import DetectedRules
from swiss_exp_tracker.pipeline_agentic.auto_detect import detect_rent
from swiss_exp_tracker.pipeline_agentic.auto_detect import detect_salary
from swiss_exp_tracker.pipeline_agentic.auto_detect import run_detection

# ─── factories ──────────────────────────────────────────────────────────────


def _make_income_row(**overrides: object) -> dict[str, object]:
    """Return a valid income transaction row dict; override any field as needed."""
    base: dict[str, object] = {
        "merchant": "Employer A",
        "amount_chf": 10000.0,
        "transaction_type": "INCOME",
        "date": "2023-01-25",
    }
    base.update(overrides)
    return base


def _make_expense_row(**overrides: object) -> dict[str, object]:
    """Return a valid expense transaction row dict; override any field as needed."""
    base: dict[str, object] = {
        "merchant": "Landlord A",
        "amount_chf": 1000.0,
        "transaction_type": "EXPENSE",
        "date": "2023-01-01",
    }
    base.update(overrides)
    return base


# ─── shared date sequences ───────────────────────────────────────────────────

# 12 payments on the 25th of each month — day std = 0, passes the <= 5 threshold.
_SALARY_DATES_25: list[str] = [
    "2023-01-25",
    "2023-02-25",
    "2023-03-25",
    "2023-04-25",
    "2023-05-25",
    "2023-06-25",
    "2023-07-25",
    "2023-08-25",
    "2023-09-25",
    "2023-10-25",
    "2023-11-25",
    "2023-12-25",
]

# 12 payments on the 15th of each month.
_SALARY_DATES_15: list[str] = [
    "2023-01-15",
    "2023-02-15",
    "2023-03-15",
    "2023-04-15",
    "2023-05-15",
    "2023-06-15",
    "2023-07-15",
    "2023-08-15",
    "2023-09-15",
    "2023-10-15",
    "2023-11-15",
    "2023-12-15",
]

# Alternating days 1 and 28 — day std ≈ 14.1, well above the threshold of 5.
_IRREGULAR_DATES: list[str] = [
    f"2023-{m:02d}-{1 if i % 2 == 0 else 28}" for i, m in enumerate(range(1, 13))
]

# Rent dates: mostly day 1, two on day 2 — day std ≈ 0.4, well within 7.
_RENT_DATES_1: list[str] = [
    "2023-01-01",
    "2023-02-01",
    "2023-03-01",
    "2023-04-01",
    "2023-05-02",
    "2023-06-01",
    "2023-07-01",
    "2023-08-01",
    "2023-09-01",
    "2023-10-02",
    "2023-11-01",
    "2023-12-01",
]

# Amounts with a high coefficient of variation (range 100-2000 CHF).
_HIGH_CV_AMOUNTS: list[float] = [
    100.0,
    200.0,
    300.0,
    500.0,
    800.0,
    1200.0,
    1500.0,
    1800.0,
    2000.0,
    250.0,
    450.0,
    700.0,
]


# ─── salary detection tests ──────────────────────────────────────────────────


def test_detect_salary_single_employer() -> None:
    """12 regular monthly income rows from one employer → that employer detected."""
    rows = [
        _make_income_row(date=d, merchant="Employer A", amount_chf=10000.0)
        for d in _SALARY_DATES_25
    ]
    df = pd.DataFrame(rows)
    result = detect_salary(df)
    assert "Employer A" in result


def test_detect_salary_two_employers() -> None:
    """Two employers with regular monthly payments → both detected."""
    rows_a = [
        _make_income_row(date=d, merchant="Employer A", amount_chf=10000.0)
        for d in _SALARY_DATES_25
    ]
    rows_b = [
        _make_income_row(date=d, merchant="Employer B", amount_chf=8000.0)
        for d in _SALARY_DATES_15
    ]
    df = pd.DataFrame(rows_a + rows_b)
    result = detect_salary(df)
    assert "Employer A" in result
    assert "Employer B" in result


def test_detect_salary_irregular_timing_excluded() -> None:
    """Merchant with irregular payment-day distribution → not detected as salary."""
    rows = [
        _make_income_row(date=d, merchant="Random Inc", amount_chf=10000.0)
        for d in _IRREGULAR_DATES
    ]
    df = pd.DataFrame(rows)
    result = detect_salary(df)
    assert "Random Inc" not in result


def test_detect_salary_empty_df() -> None:
    """Empty DataFrame → empty list, no exception."""
    df = pd.DataFrame(columns=["merchant", "amount_chf", "transaction_type", "date"])
    result = detect_salary(df)
    assert result == []


def test_detect_salary_payment_service_excluded() -> None:
    """Transactions with category_main=Payment Services → excluded from salary detection."""
    rows = [
        _make_income_row(
            date=d, merchant="UBS Bank", amount_chf=10000.0, category_main="Payment Services"
        )
        for d in _SALARY_DATES_25
    ]
    df = pd.DataFrame(rows)
    result = detect_salary(df)
    assert "UBS Bank" not in result


def test_detect_salary_below_count_threshold() -> None:
    """Fewer than 3 rows for a merchant → not detected."""
    rows = [
        _make_income_row(date=d, merchant="Rare Employer", amount_chf=10000.0)
        for d in _SALARY_DATES_25[:2]
    ]
    df = pd.DataFrame(rows)
    result = detect_salary(df)
    assert "Rare Employer" not in result


def test_detect_salary_expense_rows_ignored() -> None:
    """Expense rows from the same merchant are not counted as salary income."""
    rows = [
        _make_expense_row(date=d, merchant="Employer A", amount_chf=10000.0)
        for d in _SALARY_DATES_25
    ]
    df = pd.DataFrame(rows)
    result = detect_salary(df)
    assert "Employer A" not in result


# ─── rent detection tests ─────────────────────────────────────────────────────


def test_detect_rent_single_landlord() -> None:
    """12 monthly near-fixed expense rows → landlord detected with correct amounts."""
    # Amounts: 10 rows at 1000, one at 990, one at 1010 → cv ≈ 0.004 << 0.15
    amounts = [1000.0] * 10 + [990.0, 1010.0]
    rows = [
        _make_expense_row(date=d, merchant="Landlord A", amount_chf=a)
        for d, a in zip(_RENT_DATES_1, amounts, strict=False)
    ]
    df = pd.DataFrame(rows)
    result = detect_rent(df)
    merchants = [r.merchant for r in result]
    assert "Landlord A" in merchants
    entry = next(r for r in result if r.merchant == "Landlord A")
    # unique_amounts contains at least the dominant rent value
    assert 1000.0 in entry.amounts


def test_detect_rent_irregular_amounts_excluded() -> None:
    """High-CV merchant amounts → excluded from rent detection."""
    rows = [
        _make_expense_row(date=d, merchant="Shop X", amount_chf=a)
        for d, a in zip(_RENT_DATES_1, _HIGH_CV_AMOUNTS, strict=False)
    ]
    df = pd.DataFrame(rows)
    result = detect_rent(df)
    merchants = [r.merchant for r in result]
    assert "Shop X" not in merchants


def test_detect_rent_empty_df() -> None:
    """Empty DataFrame → empty list, no exception."""
    df = pd.DataFrame(columns=["merchant", "amount_chf", "transaction_type", "date"])
    result = detect_rent(df)
    assert result == []


def test_detect_rent_below_median_threshold() -> None:
    """Merchant with median amount < 400 CHF → not detected as rent."""
    rows = [
        _make_expense_row(date=d, merchant="Small Expense", amount_chf=50.0)
        for d in _RENT_DATES_1
    ]
    df = pd.DataFrame(rows)
    result = detect_rent(df)
    merchants = [r.merchant for r in result]
    assert "Small Expense" not in merchants


def test_detect_rent_below_count_threshold() -> None:
    """Fewer than 3 rows → not detected as rent."""
    rows = [
        _make_expense_row(date=d, merchant="Rare Landlord", amount_chf=1000.0)
        for d in _RENT_DATES_1[:2]
    ]
    df = pd.DataFrame(rows)
    result = detect_rent(df)
    merchants = [r.merchant for r in result]
    assert "Rare Landlord" not in merchants


def test_detect_rent_income_rows_ignored() -> None:
    """Income rows with rent-like amounts are not detected as rent."""
    rows = [
        _make_income_row(date=d, merchant="Landlord A", amount_chf=1000.0)
        for d in _RENT_DATES_1
    ]
    df = pd.DataFrame(rows)
    result = detect_rent(df)
    merchants = [r.merchant for r in result]
    assert "Landlord A" not in merchants


# ─── run_detection integration test ─────────────────────────────────────────


def test_run_detection_writes_file(tmp_path: Path) -> None:
    """run_detection writes a YAML file that parses back to DetectedRules."""
    output_path = tmp_path / "detected.yaml"
    rows: list[dict[str, object]] = [
        _make_income_row(date=d, merchant="Employer X", amount_chf=9000.0)
        for d in _SALARY_DATES_25
    ]
    rows += [
        _make_expense_row(date=d, merchant="Landlord B", amount_chf=1500.0)
        for d in _RENT_DATES_1
    ]
    df = pd.DataFrame(rows)

    detected = run_detection(df, output_path=output_path)

    assert output_path.exists()
    with output_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    parsed = DetectedRules.model_validate(data)
    # Detected employers and rent must match what was returned
    assert parsed.salary.employers == detected.salary.employers
    assert len(parsed.housing.rent) == len(detected.housing.rent)


def test_run_detection_salary_in_file(tmp_path: Path) -> None:
    """run_detection detects Employer X and writes it into the output YAML."""
    output_path = tmp_path / "detected.yaml"
    rows = [
        _make_income_row(date=d, merchant="Employer X", amount_chf=9000.0)
        for d in _SALARY_DATES_25
    ]
    df = pd.DataFrame(rows)

    run_detection(df, output_path=output_path)

    with output_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    parsed = DetectedRules.model_validate(data)
    assert "Employer X" in parsed.salary.employers
