"""Post-processing step for the agentic pipeline output.

Reads the latest enriched rows from ``merchant_metadata_raw``, applies
manual corrections for merchants that the agent got wrong, and upserts
the results into ``merchant_metadata_rfn``.

Add corrections to the ``CORRECTIONS`` dict:
    "matched_merchant value" -> (category_main, category_second, city_override)

Set ``city_override`` to ``None`` to keep the original city from the raw row.
"""

from __future__ import annotations

import logging
import os
import sqlite3

from swiss_exp_tracker.config.user_config_loader import load as load_config
from swiss_exp_tracker.config.user_config_schema import MergedConfig
from swiss_exp_tracker.db.sql import agentic
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategorySecond

oj = os.path.join

logger = logging.getLogger(__name__)

_cfg: MergedConfig = load_config()


def _build_containment_corrections(
    cfg: MergedConfig,
) -> dict[str, tuple[str, str, str | None]]:
    """Return lowercase merchant substring → (category_main, category_second, city) for personal entries."""
    result: dict[str, tuple[str, str, str | None]] = {}
    for employer in cfg.salary.employers:
        result[employer.lower()] = (
            CategoryMain.SALARY.value,
            CategorySecond.SALARY_MAIN.value,
            None,
        )
    for donation in cfg.salary.donations:
        result[donation.name.lower()] = (
            CategoryMain.SALARY.value,
            CategorySecond.SALARY_DONATION.value,
            None,
        )
    for rent in cfg.housing.rent:
        result[rent.merchant.lower()] = (
            CategoryMain.HOUSING.value,
            CategorySecond.HOUSING_RENT.value,
            None,
        )
    for shared in cfg.housing.shared_housing:
        result[shared.merchant.lower()] = (
            CategoryMain.HOUSING.value,
            CategorySecond.HOUSING_RENT.value,
            None,
        )
    for deposit in cfg.housing.deposits:
        result[deposit.merchant.lower()] = (
            CategoryMain.HOUSING.value,
            CategorySecond.HOUSING_DEPOSIT.value,
            None,
        )
    for rule in cfg.custom_rules:
        if not rule.exact_match:
            result[rule.merchant.lower()] = (
                rule.category_main,
                rule.category_second,
                None,
            )
    return result


def _build_exact_corrections(
    cfg: MergedConfig,
) -> dict[str, tuple[str, str, str | None]]:
    """Return lowercase merchant → (category_main, category_second, city) for exact-match rules."""
    return {
        rule.merchant.lower(): (rule.category_main, rule.category_second, None)
        for rule in cfg.custom_rules
        if rule.exact_match
    }


# Exact-match corrections: matched_merchant (case-insensitive) -> (category_main, category_second, city_override)
CORRECTIONS: dict[str, tuple[str, str, str | None]] = _build_exact_corrections(_cfg)

# Containment corrections: if any substring in the list is contained in matched_merchant,
# the correction is applied (case-insensitive).
BANK_KEYWORDS: list[str] = [
    "kantonalbank",
    "zkb",
    "raiffeisen",
    "postfinance",
    "ubs ag",
    "credit suisse",
    "ubs bank",
]

_personal_corrections = _build_containment_corrections(_cfg)
CONTAINMENT_CORRECTIONS: list[tuple[list[str], tuple[str, str, str | None]]] = [
    (
        BANK_KEYWORDS,
        (CategoryMain.PAYMENT_SERVICES.value, CategorySecond.PAYMENT_FEES.value, None),
    ),
    *[
        ([merchant], correction)
        for merchant, correction in _personal_corrections.items()
    ],
]


def _build_reference_id_corrections(
    cfg: MergedConfig,
) -> dict[str, tuple[str, str, str, str | None]]:
    """Return reference_id → (merchant, category_main, category_second, city) from user config."""
    return {
        c.reference_id: (c.merchant, c.category_main, c.category_second, c.city)
        for c in cfg.reference_id_corrections
    }


# Per-reference-id corrections: overrides matched_merchant name AND categories.
# Keyed by reference_id (the value in transactions_use.reference / merchant_metadata.reference_id).
# Value: (merchant_name, category_main, category_second, city_override or None to keep existing).
REFERENCE_ID_CORRECTIONS: dict[str, tuple[str, str, str, str | None]] = (
    _build_reference_id_corrections(_cfg)
)


def _build_exact_renames(cfg: MergedConfig) -> dict[str, str]:
    """Return lowercase merchant → new display name for exact-match renames."""
    return {r.match.lower(): r.rename_to for r in cfg.merchant_renames if r.exact_match}


def _build_containment_renames(cfg: MergedConfig) -> list[tuple[str, str]]:
    """Return (lowercase substring, new display name) pairs for containment renames."""
    return [
        (r.match.lower(), r.rename_to)
        for r in cfg.merchant_renames
        if not r.exact_match
    ]


# Merchant renames: rewrite matched_merchant without touching categories.
# Exact renames key on the full lowercase merchant; containment renames match a substring.
MERCHANT_RENAMES_EXACT: dict[str, str] = _build_exact_renames(_cfg)
MERCHANT_RENAMES_CONTAINMENT: list[tuple[str, str]] = _build_containment_renames(_cfg)


def run_post_clean() -> None:
    """Read merchant_metadata_raw, apply corrections, write merchant_metadata_rfn.

    - Takes the latest raw row per ``reference_id`` (highest ``id``).
    - New reference_ids are inserted; existing rfn rows are updated when the
      raw entry is newer (higher ``id``) than when rfn was last written.
    """
    path_db = oj("./database", "transactions.db")

    with sqlite3.connect(path_db) as db:
        db.row_factory = sqlite3.Row

        # Ensure destination table exists
        agentic.create_merchant_metadata_rfn_table(db)

        db.row_factory = None
        table_exists = agentic.check_merchant_metadata_raw_exists(db)
        db.row_factory = sqlite3.Row
        if not table_exists:
            logger.warning("post_clean: merchant_metadata_raw not found, skipping")
            return

        # Latest raw row per reference_id — include rows not yet in rfn
        # AND rows where the raw entry is newer than the existing rfn row
        raw_rows = agentic.get_latest_raw_merchant_rows(db)

        corrections_lookup = {k.lower(): v for k, v in CORRECTIONS.items()}
        containment_corrections = [
            ([s.lower() for s in patterns], correction)
            for patterns, correction in CONTAINMENT_CORRECTIONS
        ]

        rows_inserted = 0

        for row in raw_rows:
            ref_id = row["reference_id"] or ""
            merchant_key = (row["matched_merchant"] or "").lower()

            # Reference-id corrections take highest priority (also overrides merchant name)
            ref_correction = REFERENCE_ID_CORRECTIONS.get(ref_id)
            if ref_correction:
                matched_merchant, category_main, category_second, city_override = (
                    ref_correction
                )
                city = city_override if city_override is not None else row["city"]
            else:
                matched_merchant = row["matched_merchant"]
                # Exact match first, then containment
                correction = corrections_lookup.get(merchant_key)
                if correction is None:
                    for patterns, corr in containment_corrections:
                        if any(p in merchant_key for p in patterns):
                            correction = corr
                            break

                if correction:
                    category_main, category_second, city_override = correction
                    city = city_override if city_override is not None else row["city"]
                else:
                    category_main = row["category_main"]
                    category_second = row["category_second"]
                    city = row["city"]

            # Remove any existing rfn row for this reference before inserting
            # the updated one (handles the upsert case for existing references).
            agentic.delete_merchant_metadata_rfn_by_reference(
                db, reference_id=row["reference_id"]
            )
            agentic.insert_merchant_metadata_rfn(
                db,
                created_at=row["created_at"],
                reference_id=row["reference_id"],
                matched_merchant=matched_merchant,
                cache_hit=row["cache_hit"],
                similarity=row["similarity"],
                search_tool=row["search_tool"],
                category_main=category_main,
                category_second=category_second,
                city=city,
            )
            rows_inserted += 1

        db.commit()

    logger.debug("post_clean: %d rows written to merchant_metadata_rfn", rows_inserted)

    # Rename first so category rules below can key on the new merchant name.
    _apply_merchant_renames()


def _apply_corrections_to_existing_rfn() -> None:
    """Apply CORRECTIONS and CONTAINMENT_CORRECTIONS to all existing rfn rows.

    Re-runs the correction logic over every row already in ``merchant_metadata_rfn``
    so that newly added rules take effect without requiring raw rows to be re-enriched.
    """
    path_db = oj("./database", "transactions.db")

    corrections_lookup = {k.lower(): v for k, v in CORRECTIONS.items()}
    containment_corrections = [
        ([s.lower() for s in patterns], correction)
        for patterns, correction in CONTAINMENT_CORRECTIONS
    ]

    with sqlite3.connect(path_db) as db:
        db.row_factory = sqlite3.Row

        db.row_factory = None
        table_exists = agentic.check_merchant_metadata_rfn_exists(db)
        db.row_factory = sqlite3.Row
        if not table_exists:
            logger.warning(
                "post_clean: merchant_metadata_rfn not found, skipping corrections"
            )
            return

        rows = agentic.get_all_merchant_metadata_rfn(db)

        rows_updated = 0
        for row in rows:
            merchant_key = (row["matched_merchant"] or "").lower()

            correction = corrections_lookup.get(merchant_key)
            if correction is None:
                for patterns, corr in containment_corrections:
                    if any(p in merchant_key for p in patterns):
                        correction = corr
                        break

            if correction is None:
                continue

            category_main, category_second, city_override = correction
            city = city_override if city_override is not None else row["city"]

            if (
                row["category_main"] == category_main
                and row["category_second"] == category_second
                and row["city"] == city
            ):
                continue  # already correct, skip

            agentic.update_merchant_metadata_rfn_categories(
                db,
                category_main=category_main,
                category_second=category_second,
                city=city,
                id=row["id"],
            )
            rows_updated += 1

        db.commit()

    logger.debug("post_clean: %d existing rfn rows corrected", rows_updated)

    _apply_reference_id_corrections()


def _apply_reference_id_corrections() -> None:
    """Apply REFERENCE_ID_CORRECTIONS to merchant_metadata_rfn and transactions_use.

    Updates matched_merchant + categories in rfn, and merchant + categories in transactions_use.
    """
    path_db = oj("./database", "transactions.db")

    with sqlite3.connect(path_db) as db:
        rows_updated = 0
        for ref_id, (
            merchant,
            category_main,
            category_second,
            city_override,
        ) in REFERENCE_ID_CORRECTIONS.items():
            agentic.update_merchant_metadata_rfn_full_by_reference(
                db,
                matched_merchant=merchant,
                category_main=category_main,
                category_second=category_second,
                city=city_override,
                reference_id=ref_id,
            )
            agentic.update_transactions_use_merchant_by_reference(
                db,
                merchant=merchant,
                category_main=category_main,
                category_second=category_second,
                city=city_override,
                reference=ref_id,
            )
            rows_updated += 1
        db.commit()

    logger.debug("post_clean: %d reference-id corrections applied", rows_updated)


def _apply_merchant_renames() -> None:
    """Rename matched_merchant in rfn and merchant in transactions_use per merchant_renames config.

    Runs before the category passes so custom_rules can key on the new name;
    categories are left untouched here — only the display name changes.
    """
    if not MERCHANT_RENAMES_EXACT and not MERCHANT_RENAMES_CONTAINMENT:
        _apply_corrections_to_existing_rfn()
        return

    path_db = oj("./database", "transactions.db")

    with sqlite3.connect(path_db) as db:
        db.row_factory = None
        table_exists = agentic.check_merchant_metadata_rfn_exists(db)
        db.row_factory = sqlite3.Row
        if not table_exists:
            logger.warning(
                "post_clean: merchant_metadata_rfn not found, skipping renames"
            )
            return

        rows = agentic.get_all_merchant_metadata_rfn(db)

        rows_renamed = 0
        for row in rows:
            merchant_key = (row["matched_merchant"] or "").lower()

            new_name = MERCHANT_RENAMES_EXACT.get(merchant_key)
            if new_name is None:
                for pattern, rename_to in MERCHANT_RENAMES_CONTAINMENT:
                    if pattern in merchant_key:
                        new_name = rename_to
                        break

            if new_name is None or new_name == row["matched_merchant"]:
                continue  # no rename rule or already renamed

            agentic.update_merchant_metadata_rfn_merchant_by_id(
                db, matched_merchant=new_name, id=row["id"]
            )
            agentic.update_transactions_use_merchant_only_by_reference(
                db, merchant=new_name, reference=row["reference_id"]
            )
            rows_renamed += 1

        db.commit()

    logger.debug("post_clean: %d merchant renames applied", rows_renamed)

    _apply_corrections_to_existing_rfn()
