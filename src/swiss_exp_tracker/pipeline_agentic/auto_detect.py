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
from swiss_exp_tracker.pipeline_ingestion.db import get_connection

_PAYMENT_SERVICES = "Payment Services"

# A rent row counts as "recurring" if its amount is within this fraction of the
# merchant's median amount — wide enough for annual rent increases, tight enough
# to exclude one-off fees/settlements booked under the same merchant.
_RENT_AMOUNT_TOL = 0.2

# transactions_use stores the CHF value in `amount`; detect_* expect `amount_chf`.
_DETECTION_SQL = (
    "SELECT merchant, amount AS amount_chf, date, transaction_type, category_main "
    "FROM transactions_use"
)


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


def _is_monthly_regular(dates: pd.Series, max_gap_std: float = 5.0) -> bool:
    """Return True if dates recur ~monthly, judged by low std of inter-payment gaps in days.
    Gap-based instead of day-of-month so end-of-month and start-of-month rent both qualify.
    """
    ordered = pd.to_datetime(dates).sort_values()
    gaps = ordered.diff().dropna().dt.days
    if len(gaps) < 2:
        return False
    median_gap = float(gaps.median())
    if not 25.0 <= median_gap <= 35.0:
        return False
    return float(gaps.std(ddof=1)) <= max_gap_std


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
    """Return RentDetected entries for merchants with a recurring fixed high-amount expense.
    Anchors on the median amount and tests count/cv/regularity over only the rows near it, so
    one-off charges a property manager books under the same name don't mask the rent. Excludes
    Payment Services; checks monthly regularity via inter-payment gaps. Sorted by amount desc.
    """
    if df.empty:
        return []

    expense_mask = df["transaction_type"].apply(lambda v: _to_type_str(v) == "EXPENSE")
    expense_df = _drop_payment_services(df[expense_mask].copy())

    if expense_df.empty:
        return []

    results: list[tuple[str, float, list[float]]] = []

    for merchant, group in expense_df.groupby("merchant"):
        median_amount = float(group["amount_chf"].median())

        if median_amount < 400.0:
            continue

        # Keep only the recurring rent-sized rows; the median is robust to the
        # minority of low one-off charges (fees, utility settlements) the same
        # property manager books under this merchant name.
        deviation = (group["amount_chf"].astype(float) - median_amount).abs()
        recurring = group[deviation <= _RENT_AMOUNT_TOL * median_amount]

        if len(recurring) < 3:
            continue

        rec_amounts = recurring["amount_chf"].to_numpy(dtype=float)
        mean_amount = float(np.mean(rec_amounts))

        if mean_amount == 0.0:
            continue

        cv = float(np.std(rec_amounts, ddof=1) / mean_amount)

        if cv <= 0.15 and _is_monthly_regular(recurring["date"]):
            unique_amounts = sorted(recurring["amount_chf"].unique().tolist())
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
    """Detect salary/rent, preserve existing credit_card_payments, write YAML, return DetectedRules."""
    existing = DetectedRules()
    if output_path.exists():
        with output_path.open(encoding="utf-8") as f:
            raw: object = yaml.safe_load(f) or {}
        existing = DetectedRules.model_validate(raw)

    employers = detect_salary(df)
    rent_list = detect_rent(df)

    detected = DetectedRules(
        salary=SalaryDetected(employers=employers),
        housing=HousingDetected(rent=rent_list),
        credit_card_payments=existing.credit_card_payments,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(detected.model_dump(), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    return detected


def run_detection_from_db(
    output_path: Path = _CONFIG_DIR / "detected_rules.yaml",
) -> DetectedRules:
    """Read transactions_use, detect salary/rent merchants, and write detected_rules.yaml.
    Returns empty rules without writing if transactions_use does not exist yet (first run).
    """
    with get_connection() as db:
        exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='transactions_use'"
        ).fetchone()
        if exists is None:
            return DetectedRules()
        df = pd.read_sql_query(_DETECTION_SQL, db)

    return run_detection(df, output_path=output_path)
