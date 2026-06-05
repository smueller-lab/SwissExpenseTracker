"""Post-processing step for the agentic pipeline output.

Reads the latest enriched rows from ``merchant_metadata_raw``, applies
manual corrections for merchants that the agent got wrong, and upserts
the results into ``merchant_metadata_rfn``.

Add corrections to the ``CORRECTIONS`` dict:
    "matched_merchant value" -> (category_main, category_second, city_override)

Set ``city_override`` to ``None`` to keep the original city from the raw row.
"""

from __future__ import annotations

import os
import sqlite3

from tqdm import tqdm

from swiss_exp_tracker.config import CAR_FUEL_1
from swiss_exp_tracker.config import HOUSING_RENT_2
from swiss_exp_tracker.config import RESTAURANT_BAKERY_1
from swiss_exp_tracker.config import RETAIL_SPORTS_1
from swiss_exp_tracker.config import SALARY_DONATION_1
from swiss_exp_tracker.config import SPORT_GOLF_1
from swiss_exp_tracker.config import work_places
from swiss_exp_tracker.db.sql import agentic
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategorySecond


oj = os.path.join

# ---------------------------------------------------------------------------
# Manual corrections
# Key   : matched_merchant (case-insensitive exact match)
# Value : (category_main, category_second, city_override or None)
# ---------------------------------------------------------------------------
# Exact-match corrections: matched_merchant (case-insensitive) -> (category_main, category_second, city_override)
CORRECTIONS: dict[str, tuple[str, str, str | None]] = {
    "revolut": (
        CategoryMain.PAYMENT_SERVICES.value,
        CategorySecond.PAYMENT_MONEY_TRANSFER.value,
        None,
    ),
    "p corporate hospitality": (
        CategoryMain.ENTERTAINMENT.value,
        CategorySecond.ENTERTAINMENT_SPORTS.value,
        None,
    ),
}

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

CONTAINMENT_CORRECTIONS: list[tuple[list[str], tuple[str, str, str | None]]] = [
    (
        BANK_KEYWORDS,
        (CategoryMain.PAYMENT_SERVICES.value, CategorySecond.PAYMENT_FEES.value, None),
    ),
    (work_places, (CategoryMain.SALARY.value, CategorySecond.SALARY_MAIN.value, None)),
    (
        SALARY_DONATION_1,
        (CategoryMain.SALARY.value, CategorySecond.SALARY_DONATION.value, None),
    ),
    (
        HOUSING_RENT_2,
        (CategoryMain.HOUSING.value, CategorySecond.HOUSING_RENT.value, None),
    ),
    (SPORT_GOLF_1, (CategoryMain.SPORT.value, CategorySecond.SPORT_GOLF.value, None)),
    (CAR_FUEL_1, (CategoryMain.CAR.value, CategorySecond.CAR_FUEL.value, None)),
    (
        RETAIL_SPORTS_1,
        (CategoryMain.RETAIL.value, CategorySecond.RETAIL_SPORTS.value, None),
    ),
    (
        RESTAURANT_BAKERY_1,
        (CategoryMain.RESTAURANT.value, CategorySecond.RESTAURANT_CAFE.value, None),
    ),
]

# Per-reference-id corrections: overrides matched_merchant name AND categories.
# Keyed by reference_id (the value in transactions_use.reference / merchant_metadata.reference_id).
# Value: (merchant_name, category_main, category_second, city_override or None to keep existing).
REFERENCE_ID_CORRECTIONS: dict[str, tuple[str, str, str, str | None]] = {
    "NOID-337ce749-2152-45e3-aa98-1083fb846006": (
        "Migros Golfpark Waldkirch",
        CategoryMain.SPORT.value,
        CategorySecond.SPORT_GOLF.value,
        "Waldkirch",
    ),
}

# SWITZERLAND FIRST
# Parkingpay App, Transport, Parking -> should be Car_Parking
# Liegenschaft Neumarkt, Housing Rent -> Idk
# Raststätte Knonauer Amt, Car Service Reapi -> should be Fueling

# Restaurant Parking is not possible
# add SPA to categories
# Any place that serve food should be restaurant except for Bar

# Car, car washing


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
            tqdm.write("post_clean: merchant_metadata_raw not found, skipping")
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

    tqdm.write(f"post_clean: {rows_inserted} rows written to merchant_metadata_rfn")

    _apply_corrections_to_existing_rfn()


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
            tqdm.write(
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

    tqdm.write(f"post_clean: {rows_updated} existing rfn rows corrected")

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

    tqdm.write(f"post_clean: {rows_updated} reference-id corrections applied")
