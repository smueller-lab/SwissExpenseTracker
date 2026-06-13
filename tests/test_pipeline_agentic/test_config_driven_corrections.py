"""Tests for config-driven correction builder functions.

Covers _build_containment_corrections (clean_pipeline_output) and
_build_amount_corrections, _build_threshold_corrections, _build_year_corrections,
_get_roommate_config (transactions_use).
"""

from __future__ import annotations

import pytest

from swiss_exp_tracker.config.user_config_schema import CustomRule
from swiss_exp_tracker.config.user_config_schema import DepositRule
from swiss_exp_tracker.config.user_config_schema import HousingMerged
from swiss_exp_tracker.config.user_config_schema import InvestingTransfer
from swiss_exp_tracker.config.user_config_schema import InvestingUser
from swiss_exp_tracker.config.user_config_schema import MergedConfig
from swiss_exp_tracker.config.user_config_schema import RentUser
from swiss_exp_tracker.config.user_config_schema import SalaryDonation
from swiss_exp_tracker.config.user_config_schema import SalaryMerged
from swiss_exp_tracker.config.user_config_schema import SharedHousingRule
from swiss_exp_tracker.config.user_config_schema import TravelAllInclusive
from swiss_exp_tracker.config.user_config_schema import TravelUser
from swiss_exp_tracker.pipeline_agentic.clean_pipeline_output import (
    _build_containment_corrections,
)
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategorySecond
from swiss_exp_tracker.pipeline_agentic.transactions_use import (
    _build_amount_corrections,
)
from swiss_exp_tracker.pipeline_agentic.transactions_use import (
    _build_threshold_corrections,
)
from swiss_exp_tracker.pipeline_agentic.transactions_use import _build_year_corrections
from swiss_exp_tracker.pipeline_agentic.transactions_use import _get_roommate_config

# ─── MergedConfig factory ────────────────────────────────────────────────────


def _make_cfg(**overrides: object) -> MergedConfig:
    """Return a minimal MergedConfig; override any top-level attribute as needed."""
    base: dict[str, object] = {
        "salary": SalaryMerged(),
        "housing": HousingMerged(),
        "investing": InvestingUser(),
        "travel": TravelUser(),
        "custom_rules": [],
    }
    base.update(overrides)
    return MergedConfig.model_validate(base)


# ─── _build_containment_corrections ─────────────────────────────────────────


def test_build_containment_two_employers() -> None:
    """Two salary employers → both appear as lowercase keys in returned dict."""
    cfg = _make_cfg(salary=SalaryMerged(employers=["Acme Corp", "Tech GmbH"]))
    result = _build_containment_corrections(cfg)

    assert "acme corp" in result
    assert "tech gmbh" in result


def test_build_containment_employer_category_values() -> None:
    """Employer key maps to Salary / Main Salary with no city override."""
    cfg = _make_cfg(salary=SalaryMerged(employers=["Acme Corp"]))
    result = _build_containment_corrections(cfg)

    cat_main, cat_second, city = result["acme corp"]
    assert cat_main == CategoryMain.SALARY.value
    assert cat_second == CategorySecond.SALARY_MAIN.value
    assert city is None


def test_build_containment_empty_employers() -> None:
    """Empty employers list → no salary category entries in result."""
    cfg = _make_cfg(salary=SalaryMerged(employers=[]))
    result = _build_containment_corrections(cfg)

    salary_entries = [k for k, v in result.items() if v[0] == CategoryMain.SALARY.value]
    assert salary_entries == []


def test_build_containment_donation_included() -> None:
    """Salary donation → appears in result with SALARY / Donation categories."""
    cfg = _make_cfg(salary=SalaryMerged(donations=[SalaryDonation(name="Good Cause")]))
    result = _build_containment_corrections(cfg)

    assert "good cause" in result
    cat_main, cat_second, city = result["good cause"]
    assert cat_main == CategoryMain.SALARY.value
    assert cat_second == CategorySecond.SALARY_DONATION.value
    assert city is None


def test_build_containment_rent_included() -> None:
    """Rent merchant → appears in result with Housing / Rent categories."""
    cfg = _make_cfg(
        housing=HousingMerged(
            rent=[RentUser(merchant="Muster Vermieter", amounts=[1200.0])]
        )
    )
    result = _build_containment_corrections(cfg)

    assert "muster vermieter" in result
    cat_main, cat_second, city = result["muster vermieter"]
    assert cat_main == CategoryMain.HOUSING.value
    assert cat_second == CategorySecond.HOUSING_RENT.value
    assert city is None


def test_build_containment_shared_housing_included() -> None:
    """Shared housing merchant → appears in result with Housing / Rent categories."""
    cfg = _make_cfg(
        housing=HousingMerged(
            shared_housing=[
                SharedHousingRule(merchant="Flatshare AG", roommate_offset=450.0)
            ]
        )
    )
    result = _build_containment_corrections(cfg)

    assert "flatshare ag" in result
    cat_main, cat_second, _ = result["flatshare ag"]
    assert cat_main == CategoryMain.HOUSING.value
    assert cat_second == CategorySecond.HOUSING_RENT.value


def test_build_containment_deposit_included() -> None:
    """Deposit merchant → appears in result with Housing / Deposit categories."""
    cfg = _make_cfg(
        housing=HousingMerged(
            deposits=[DepositRule(merchant="Deposit GmbH", min_amount=3000.0)]
        )
    )
    result = _build_containment_corrections(cfg)

    assert "deposit gmbh" in result
    cat_main, cat_second, city = result["deposit gmbh"]
    assert cat_main == CategoryMain.HOUSING.value
    assert cat_second == CategorySecond.HOUSING_DEPOSIT.value
    assert city is None


def test_build_containment_custom_rule_included() -> None:
    """Custom rule → appears in result with the user-specified category values."""
    cfg = _make_cfg(
        custom_rules=[
            CustomRule(
                merchant="My Gym", category_main="Sport", category_second="Fitness"
            )
        ]
    )
    result = _build_containment_corrections(cfg)

    assert "my gym" in result
    cat_main, cat_second, city = result["my gym"]
    assert cat_main == "Sport"
    assert cat_second == "Fitness"
    assert city is None


def test_build_containment_keys_are_lowercase() -> None:
    """Keys in the returned dict are always lowercase regardless of input case."""
    cfg = _make_cfg(salary=SalaryMerged(employers=["UPPERCASE CORP"]))
    result = _build_containment_corrections(cfg)

    assert "uppercase corp" in result
    assert "UPPERCASE CORP" not in result


def test_build_containment_empty_config_returns_empty() -> None:
    """Completely empty config → empty correction dict."""
    cfg = _make_cfg()
    result = _build_containment_corrections(cfg)
    assert result == {}


# ─── _build_amount_corrections ───────────────────────────────────────────────


def test_build_amount_corrections_two_rent_entries() -> None:
    """Two rent entries with amounts → both appear in the returned list."""
    cfg = _make_cfg(
        housing=HousingMerged(
            rent=[
                RentUser(merchant="Landlord A", amounts=[900.0, 1000.0]),
                RentUser(merchant="Landlord B", amounts=[1500.0]),
            ]
        )
    )
    result = _build_amount_corrections(cfg)

    merchants = [patterns[0] for patterns, _amounts, _cats in result]
    assert "Landlord A" in merchants
    assert "Landlord B" in merchants


def test_build_amount_corrections_amounts_preserved() -> None:
    """Amounts list is passed through unchanged for each rent entry."""
    cfg = _make_cfg(
        housing=HousingMerged(
            rent=[RentUser(merchant="Landlord A", amounts=[900.0, 1000.0])]
        )
    )
    result = _build_amount_corrections(cfg)

    entry = next(
        (e for e in result if e[0][0] == "Landlord A"),
        None,
    )
    assert entry is not None
    _patterns, amounts, _cats = entry
    assert amounts == pytest.approx([900.0, 1000.0])


def test_build_amount_corrections_categories_are_housing_rent() -> None:
    """Category tuple for each rent entry is (Housing, Rent)."""
    cfg = _make_cfg(
        housing=HousingMerged(rent=[RentUser(merchant="Landlord A", amounts=[1000.0])])
    )
    result = _build_amount_corrections(cfg)

    _patterns, _amounts, (cat_main, cat_second) = result[0]
    assert cat_main == CategoryMain.HOUSING.value
    assert cat_second == CategorySecond.HOUSING_RENT.value


def test_build_amount_corrections_rent_without_amounts_excluded() -> None:
    """Rent entry with no amounts (None) → not included in correction list."""
    cfg = _make_cfg(
        housing=HousingMerged(rent=[RentUser(merchant="No-Amount Landlord")])
    )
    result = _build_amount_corrections(cfg)

    merchants = [patterns[0] for patterns, _amounts, _cats in result]
    assert "No-Amount Landlord" not in merchants


def test_build_amount_corrections_empty_config() -> None:
    """Config with no rent entries → empty list returned."""
    cfg = _make_cfg()
    result = _build_amount_corrections(cfg)
    assert result == []


# ─── _build_threshold_corrections ───────────────────────────────────────────


def test_build_threshold_one_investing() -> None:
    """One investing transfer entry → correct merchant and min_amount in result."""
    cfg = _make_cfg(
        investing=InvestingUser(
            transfers=[InvestingTransfer(merchant="My Brokerage", min_amount=1000.0)]
        )
    )
    result = _build_threshold_corrections(cfg)

    assert len(result) == 1
    patterns, min_amount, (cat_main, cat_second) = result[0]
    assert "My Brokerage" in patterns
    assert min_amount == pytest.approx(1000.0)
    assert cat_main == CategoryMain.INVESTING.value
    assert cat_second == CategorySecond.INVESTING_BROKERAGE.value


def test_build_threshold_two_investing_entries() -> None:
    """Two investing transfers → both appear in result."""
    cfg = _make_cfg(
        investing=InvestingUser(
            transfers=[
                InvestingTransfer(merchant="My Brokerage", min_amount=1000.0),
                InvestingTransfer(merchant="Second Brokerage", min_amount=500.0),
            ]
        )
    )
    result = _build_threshold_corrections(cfg)

    all_merchants = [p for patterns, _m, _cats in result for p in patterns]
    assert "My Brokerage" in all_merchants
    assert "Second Brokerage" in all_merchants


def test_build_threshold_one_deposit() -> None:
    """One housing deposit entry → correct merchant and min_amount in result."""
    cfg = _make_cfg(
        housing=HousingMerged(
            deposits=[DepositRule(merchant="Deposit AG", min_amount=2500.0)]
        )
    )
    result = _build_threshold_corrections(cfg)

    assert len(result) == 1
    patterns, min_amount, (cat_main, cat_second) = result[0]
    assert "Deposit AG" in patterns
    assert min_amount == pytest.approx(2500.0)
    assert cat_main == CategoryMain.HOUSING.value
    assert cat_second == CategorySecond.HOUSING_DEPOSIT.value


def test_build_threshold_investing_before_deposits() -> None:
    """Investing entries appear before deposit entries in the result list."""
    cfg = _make_cfg(
        investing=InvestingUser(
            transfers=[InvestingTransfer(merchant="My Brokerage", min_amount=1000.0)]
        ),
        housing=HousingMerged(
            deposits=[DepositRule(merchant="Deposit AG", min_amount=2500.0)]
        ),
    )
    result = _build_threshold_corrections(cfg)

    assert len(result) == 2
    # First entry must be the investing transfer
    first_patterns, _m, (first_main, _s) = result[0]
    assert first_main == CategoryMain.INVESTING.value
    assert "My Brokerage" in first_patterns


def test_build_threshold_empty_config() -> None:
    """Config with no investing or deposit entries → empty list returned."""
    cfg = _make_cfg()
    result = _build_threshold_corrections(cfg)
    assert result == []


# ─── _build_year_corrections ─────────────────────────────────────────────────


def test_build_year_one_travel() -> None:
    """One travel all_inclusive entry → correct merchant and year in result."""
    cfg = _make_cfg(
        travel=TravelUser(
            all_inclusive=[TravelAllInclusive(merchant="Sunny Travels", year=2024)]
        )
    )
    result = _build_year_corrections(cfg)

    assert len(result) == 1
    patterns, year, (cat_main, cat_second) = result[0]
    assert "Sunny Travels" in patterns
    assert year == 2024
    assert cat_main == CategoryMain.TRAVEL.value
    assert cat_second == CategorySecond.TRAVEL_ALL_INCLUSIVE.value


def test_build_year_two_travel_entries() -> None:
    """Two travel entries → both appear in result with correct years."""
    cfg = _make_cfg(
        travel=TravelUser(
            all_inclusive=[
                TravelAllInclusive(merchant="Sunny Travels", year=2024),
                TravelAllInclusive(merchant="Beach Holidays", year=2023),
            ]
        )
    )
    result = _build_year_corrections(cfg)

    assert len(result) == 2
    years_by_merchant = {patterns[0]: year for patterns, year, _cats in result}
    assert years_by_merchant["Sunny Travels"] == 2024
    assert years_by_merchant["Beach Holidays"] == 2023


def test_build_year_empty_config() -> None:
    """Config with no travel entries → empty list returned."""
    cfg = _make_cfg()
    result = _build_year_corrections(cfg)
    assert result == []


# ─── _get_roommate_config ─────────────────────────────────────────────────────


def test_get_roommate_config_returns_shared_housing() -> None:
    """Shared housing list is returned unchanged from config."""
    rule = SharedHousingRule(merchant="Flatshare AG", roommate_offset=450.0)
    cfg = _make_cfg(housing=HousingMerged(shared_housing=[rule]))

    result = _get_roommate_config(cfg)

    assert len(result) == 1
    assert result[0].merchant == "Flatshare AG"
    assert result[0].roommate_offset == pytest.approx(450.0)


def test_get_roommate_config_empty_returns_empty_list() -> None:
    """Config with no shared housing → empty list returned."""
    cfg = _make_cfg()
    result = _get_roommate_config(cfg)
    assert result == []
