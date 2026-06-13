from __future__ import annotations

from swiss_exp_tracker.config.user_config_loader import load as _load_config
from swiss_exp_tracker.config.user_config_schema import MergedConfig

merged_cfg: MergedConfig = _load_config()

# ── Backward-compatible exports ───────────────────────────────────────────────
# All previously hardcoded personal constants are now derived from merged_cfg.
# These exports keep clean_pipeline_output.py and transactions_use.py working
# until Builder Task 3 migrates them to consume MergedConfig directly.

work_places: list[str] = merged_cfg.salary.employers

SALARY_DONATION_1: list[str] = [d.name for d in merged_cfg.salary.donations]

SPORT_GOLF_1: list[str] = [
    r.merchant
    for r in merged_cfg.custom_rules
    if r.category_main == "Sport" and r.category_second == "Golf"
]
CAR_FUEL_1: list[str] = [
    r.merchant
    for r in merged_cfg.custom_rules
    if r.category_main == "Car" and r.category_second == "Fuel"
]
RETAIL_SPORTS_1: list[str] = [
    r.merchant
    for r in merged_cfg.custom_rules
    if r.category_main == "Retail" and r.category_second == "Sport"
]
RESTAURANT_BAKERY_1: list[str] = [
    r.merchant for r in merged_cfg.custom_rules if r.category_second == "Cafe"
]

HOUSING_RENT_2: list[str] = [r.merchant for r in merged_cfg.housing.shared_housing]
HOUSING_RENT_2_ROOMMATE_OFFSET: float = (
    merged_cfg.housing.shared_housing[0].roommate_offset
    if merged_cfg.housing.shared_housing
    else 0.0
)

HOUSING_DEPOSIT_1: list[str] = [d.merchant for d in merged_cfg.housing.deposits]
HOUSING_DEPOSIT_1_MIN_AMOUNT: float = (
    merged_cfg.housing.deposits[0].min_amount if merged_cfg.housing.deposits else 0.0
)

INVESTING_BROKERAGE_1: list[str] = [t.merchant for t in merged_cfg.investing.transfers]
INVESTING_BROKERAGE_1_MIN_AMOUNT: float = (
    merged_cfg.investing.transfers[0].min_amount or 0.0
    if merged_cfg.investing.transfers
    else 0.0
)

TRAVEL_ALL_INCLUSIVE_1: list[str] = [
    t.merchant for t in merged_cfg.travel.all_inclusive
]
TRAVEL_ALL_INCLUSIVE_1_YEAR: int = (
    merged_cfg.travel.all_inclusive[0].year or 0
    if merged_cfg.travel.all_inclusive
    else 0
)

_rent_with_amounts = [r for r in merged_cfg.housing.rent if r.amounts is not None]
_first_rent = _rent_with_amounts[0] if _rent_with_amounts else None
_second_rent = _rent_with_amounts[1] if len(_rent_with_amounts) > 1 else None
_first_rent_amounts = _first_rent.amounts if _first_rent is not None else None
_second_rent_amounts = _second_rent.amounts if _second_rent is not None else None

HOUSING_RENT_1: list[str] = [_first_rent.merchant] if _first_rent is not None else []
HOUSING_RENT_AMOUNTS_1: list[float] = (
    _first_rent_amounts if _first_rent_amounts is not None else []
)
HOUSING_RENT_3: list[str] = [_second_rent.merchant] if _second_rent is not None else []
HOUSING_RENT_3_AMOUNTS: list[float] = (
    _second_rent_amounts if _second_rent_amounts is not None else []
)
