from __future__ import annotations

from enum import Enum
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from swiss_exp_tracker.config.user_config_schema import DetectedRules
from swiss_exp_tracker.config.user_config_schema import HousingDetected
from swiss_exp_tracker.config.user_config_schema import RentDetected
from swiss_exp_tracker.config.user_config_schema import SalaryDetected

_PAYMENT_SERVICES = "Payment Services"


def _to_type_str(value: str | Enum) -> str:
    """Return the uppercase string representation of a transaction_type, handling str and enum."""
    if isinstance(value, Enum):
        return str(value.value).upper()
    return str(value).upper()


def _drop_payment_services(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows whose category_main is Payment Services; no-op if column is absent."""
    if "category_main" not in df.columns:
        return df
    return df[df["category_main"] != _PAYMENT_SERVICES]


def detect_salary(df: pd.DataFrame) -> list[str]:
    """Return merchant names that look like salary sources based on regularity and amount size.
    Excludes Payment Services transactions using category_main.
    """
    if df.empty:
        return []

    income_mask = df["transaction_type"].apply(lambda v: _to_type_str(v) == "INCOME")
    income_df = _drop_payment_services(df[income_mask].copy())

    if income_df.empty:
        return []

    income_df["_day"] = pd.to_datetime(income_df["date"]).dt.day

    candidates: dict[str, float] = {}

    for merchant, group in income_df.groupby("merchant"):
        count = len(group)
        median_amount = float(group["amount_chf"].median())
        day_std = float(group["_day"].std(ddof=1)) if len(group) > 1 else 0.0

        if count >= 3 and day_std <= 5:
            candidates[str(merchant)] = median_amount

    if not candidates:
        return []

    max_median = max(candidates.values())
    threshold = 0.5 * max_median

    return [m for m, med in candidates.items() if med >= threshold]


def detect_rent(df: pd.DataFrame) -> list[RentDetected]:
    """Return RentDetected entries for merchants matching recurring fixed high-amount expense patterns.
    Excludes Payment Services transactions using category_main. Results sorted by median_amount descending.
    """
    if df.empty:
        return []

    expense_mask = df["transaction_type"].apply(lambda v: _to_type_str(v) == "EXPENSE")
    expense_df = _drop_payment_services(df[expense_mask].copy())

    if expense_df.empty:
        return []

    expense_df["_day"] = pd.to_datetime(expense_df["date"]).dt.day

    results: list[tuple[str, float, list[float]]] = []

    for merchant, group in expense_df.groupby("merchant"):
        count = len(group)
        amounts = group["amount_chf"].to_numpy(dtype=float)
        mean_amount = float(np.mean(amounts))

        if mean_amount == 0.0:
            continue

        median_amount = float(np.median(amounts))
        cv = float(np.std(amounts, ddof=1) / mean_amount) if len(group) > 1 else 0.0
        day_std = float(group["_day"].std(ddof=1)) if len(group) > 1 else 0.0

        if count >= 3 and cv <= 0.15 and median_amount >= 400.0 and day_std <= 7:
            unique_amounts = sorted(group["amount_chf"].unique().tolist())
            results.append((str(merchant), median_amount, unique_amounts))

    results.sort(key=lambda x: x[1], reverse=True)
    return [
        RentDetected(merchant=merchant, amounts=amounts)
        for merchant, _, amounts in results
    ]


_CONFIG_DIR = Path(__file__).parent / "config"


def run_detection(
    df: pd.DataFrame,
    output_path: Path = _CONFIG_DIR / "detected_rules.yaml",
) -> DetectedRules:
    """Detect salary and rent merchants, write YAML to output_path, and return DetectedRules."""
    employers = detect_salary(df)
    rent_list = detect_rent(df)

    detected = DetectedRules(
        salary=SalaryDetected(employers=employers),
        housing=HousingDetected(rent=rent_list),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(detected.model_dump(), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    return detected
