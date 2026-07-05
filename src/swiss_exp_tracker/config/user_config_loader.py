from __future__ import annotations

from pathlib import Path

import yaml

from swiss_exp_tracker.config.user_config_schema import DetectedRules
from swiss_exp_tracker.config.user_config_schema import HousingMerged
from swiss_exp_tracker.config.user_config_schema import MergedConfig
from swiss_exp_tracker.config.user_config_schema import RentUser
from swiss_exp_tracker.config.user_config_schema import SalaryMerged
from swiss_exp_tracker.config.user_config_schema import UserConfig

_CONFIG_DIR = Path(__file__).parent.parent / "pipeline_agentic" / "config"


def load(
    user_config_path: Path = _CONFIG_DIR / "user_config.yaml",
    detected_path: Path = _CONFIG_DIR / "detected_rules.yaml",
) -> MergedConfig:
    """Load and merge user_config.yaml and detected_rules.yaml into MergedConfig."""
    user = _load_user_config(user_config_path)
    detected = _load_detected_rules(detected_path)
    return _merge(user, detected)


def _load_user_config(path: Path) -> UserConfig:
    """Parse user_config.yaml into UserConfig; return empty model if file is absent."""
    if not path.exists():
        return UserConfig()
    with path.open(encoding="utf-8") as f:
        data: object = yaml.safe_load(f) or {}
    return UserConfig.model_validate(data)


def _load_detected_rules(path: Path) -> DetectedRules:
    """Parse detected_rules.yaml into DetectedRules; return empty model if file is absent."""
    if not path.exists():
        return DetectedRules()
    with path.open(encoding="utf-8") as f:
        data: object = yaml.safe_load(f) or {}
    return DetectedRules.model_validate(data)


def load_credit_card_payment_texts(
    detected_path: Path = _CONFIG_DIR / "detected_rules.yaml",
) -> list[str]:
    """Return booking texts for detected credit-card payments from detected_rules.yaml.
    Returns [] when the file is absent or the section is empty.
    """
    return _load_detected_rules(detected_path).credit_card_payments.booking_texts


def load_rent_exclude(
    user_config_path: Path = _CONFIG_DIR / "user_config.yaml",
) -> list[str]:
    """Return the housing.rent_exclude merchant list from user_config.yaml; [] if absent."""
    return _load_user_config(user_config_path).housing.rent_exclude


def _merge(user: UserConfig, detected: DetectedRules) -> MergedConfig:
    """Merge user and detected configs: detected provides defaults, user overrides."""
    employers = _merge_employers(user, detected)
    rent = _merge_rent(user, detected)

    return MergedConfig(
        salary=SalaryMerged(
            employers=employers,
            donations=user.salary.donations,
        ),
        housing=HousingMerged(
            rent=rent,
            deposits=user.housing.deposits,
            shared_housing=user.housing.shared_housing,
        ),
        investing=user.investing,
        travel=user.travel,
        custom_rules=user.custom_rules,
        reference_id_corrections=user.reference_id_corrections,
        merchant_renames=user.merchant_renames,
    )


def _merge_employers(user: UserConfig, detected: DetectedRules) -> list[str]:
    """Return union of detected and user employers minus employers_exclude, preserving order."""
    excluded = set(user.salary.employers_exclude)
    seen: dict[str, None] = {}
    for employer in detected.salary.employers + user.salary.employers:
        if employer not in excluded:
            seen[employer] = None
    return list(seen.keys())


def _merge_rent(user: UserConfig, detected: DetectedRules) -> list[RentUser]:
    """Merge rent entries by merchant key: user entry wins; drop rent_exclude merchants."""
    rent_exclude = set(user.housing.rent_exclude)
    rent_map: dict[str, RentUser] = {}
    for detected_r in detected.housing.rent:
        if detected_r.merchant not in rent_exclude:
            rent_map[detected_r.merchant] = RentUser(
                merchant=detected_r.merchant,
                amounts=detected_r.amounts if detected_r.amounts else None,
            )
    for user_r in user.housing.rent:
        if user_r.merchant not in rent_exclude:
            rent_map[user_r.merchant] = user_r
    return list(rent_map.values())
