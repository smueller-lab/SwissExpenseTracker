"""Build the final analysis table ``transactions_use``.

Joins ``transactions_rfn`` (enriched transactions) with ``merchant_metadata_rfn``
(cleaned merchant categories) on ``reference = reference_id`` and writes the
result into ``transactions_use``.

Only transactions whose ``enrichment_status = 'enriched'`` are included, and
only those not already present in the destination table (idempotent on re-runs).
"""

from __future__ import annotations

import logging
import sqlite3

from swiss_exp_tracker.config.user_config_loader import load as load_config
from swiss_exp_tracker.config.user_config_schema import MergedConfig
from swiss_exp_tracker.config.user_config_schema import SharedHousingRule
from swiss_exp_tracker.db.sql import agentic
from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategorySecond
from swiss_exp_tracker.pipeline_ingestion.db import get_connection

logger = logging.getLogger(__name__)

_cfg: MergedConfig = load_config()


def _build_amount_corrections(
    cfg: MergedConfig,
) -> list[tuple[list[str], list[float], tuple[str, str]]]:
    """Return rent merchant → exact-amount list entries for Housing/Rent category overrides."""
    result: list[tuple[list[str], list[float], tuple[str, str]]] = []
    for rent in cfg.housing.rent:
        if rent.amounts:
            result.append(
                (
                    [rent.merchant],
                    rent.amounts,
                    (CategoryMain.HOUSING.value, CategorySecond.HOUSING_RENT.value),
                )
            )
    return result


def _build_date_corrections(
    cfg: MergedConfig,
) -> list[tuple[list[str], list[str], tuple[str, str]]]:
    """Return merchant → date list entries for date-specific category overrides."""
    result: list[tuple[list[str], list[str], tuple[str, str]]] = []
    for transfer in cfg.investing.transfers:
        if transfer.dates:
            result.append(
                (
                    [transfer.merchant],
                    [d[:10] for d in transfer.dates],
                    (
                        CategoryMain.INVESTING.value,
                        CategorySecond.INVESTING_BROKERAGE.value,
                    ),
                )
            )
    for travel in cfg.travel.all_inclusive:
        if travel.dates:
            result.append(
                (
                    [travel.merchant],
                    [d[:10] for d in travel.dates],
                    (
                        CategoryMain.TRAVEL.value,
                        CategorySecond.TRAVEL_ALL_INCLUSIVE.value,
                    ),
                )
            )
    return result


def _build_threshold_corrections(
    cfg: MergedConfig,
) -> list[tuple[list[str], float, tuple[str, str]]]:
    """Return merchant → min_amount entries for Investing/Brokerage and Housing/Deposit overrides."""
    result: list[tuple[list[str], float, tuple[str, str]]] = []
    for transfer in cfg.investing.transfers:
        if transfer.min_amount is not None:
            result.append(
                (
                    [transfer.merchant],
                    transfer.min_amount,
                    (
                        CategoryMain.INVESTING.value,
                        CategorySecond.INVESTING_BROKERAGE.value,
                    ),
                )
            )
    for deposit in cfg.housing.deposits:
        result.append(
            (
                [deposit.merchant],
                deposit.min_amount,
                (CategoryMain.HOUSING.value, CategorySecond.HOUSING_DEPOSIT.value),
            )
        )
    return result


def _build_year_corrections(
    cfg: MergedConfig,
) -> list[tuple[list[str], int, tuple[str, str]]]:
    """Return travel merchant → year entries for Travel/AllInclusive category overrides."""
    result: list[tuple[list[str], int, tuple[str, str]]] = []
    for travel in cfg.travel.all_inclusive:
        if travel.year is not None:
            result.append(
                (
                    [travel.merchant],
                    travel.year,
                    (
                        CategoryMain.TRAVEL.value,
                        CategorySecond.TRAVEL_ALL_INCLUSIVE.value,
                    ),
                )
            )
    return result


def _get_roommate_config(cfg: MergedConfig) -> list[SharedHousingRule]:
    """Return list of shared_housing configs (merchant, roommate_offset)."""
    return cfg.housing.shared_housing


# Merchant-name substrings + exact amounts → (category_main, category_second)
AMOUNT_CORRECTIONS: list[tuple[list[str], list[float], tuple[str, str]]] = (
    _build_amount_corrections(_cfg)
)

# Merchant-name substrings + specific dates (YYYY-MM-DD) → (category_main, category_second)
DATE_CORRECTIONS: list[tuple[list[str], list[str], tuple[str, str]]] = (
    _build_date_corrections(_cfg)
)

# Merchant-name substrings + minimum amount (exclusive) → (category_main, category_second)
AMOUNT_THRESHOLD_CORRECTIONS: list[tuple[list[str], float, tuple[str, str]]] = (
    _build_threshold_corrections(_cfg)
)

# Merchant-name substrings + exact year → (category_main, category_second)
YEAR_CORRECTIONS: list[tuple[list[str], int, tuple[str, str]]] = (
    _build_year_corrections(_cfg)
)

_roommate_configs = _get_roommate_config(_cfg)
HOUSING_RENT_2: list[str] = [r.merchant for r in _roommate_configs]
HOUSING_RENT_2_ROOMMATE_OFFSET: float = (
    _roommate_configs[0].roommate_offset if _roommate_configs else 0.0
)


def run_transactions_use() -> None:
    """Join transactions_rfn + merchant_metadata_rfn → transactions_use."""
    with get_connection() as db:
        db.row_factory = sqlite3.Row

        agentic.create_transactions_use_table(db)

        use_columns = {
            str(col[1]) for col in agentic.get_transactions_use_column_names(db)
        }
        if "balance_chf" not in use_columns:
            db.execute(agentic.alter_transactions_use_add_balance_chf.sql)

        rows = agentic.get_enriched_transactions_not_in_use(db)

        rows_to_insert = [dict(row) for row in rows]
        agentic.insert_transactions_use(db, rows_to_insert)

        # transactions_use is append-only, so rows deleted from rfn after a prior run
        # (e.g. credit-card bill payments dropped in postprocess) would otherwise linger
        # in the analysis table and the dashboard. Remove those orphans here.
        orphans_removed = agentic.delete_transactions_use_orphans(db)

        db.commit()

    logger.debug(
        "transactions_use: %d rows written, %d orphans removed",
        len(rows_to_insert),
        orphans_removed,
    )

    _backfill_balance_chf_use()
    _sync_categories_from_rfn()


def _sync_categories_from_rfn() -> None:
    """Update existing transactions_use rows whose rfn categories changed."""
    with get_connection() as db:
        db.row_factory = sqlite3.Row

        rows = list(agentic.get_transactions_use_sync_candidates(db))

        for row in rows:
            agentic.update_transactions_use_categories(
                db,
                category_main=row["category_main"],
                category_second=row["category_second"],
                city=row["city"],
                id=row["id"],
            )

        db.commit()

    logger.debug("transactions_use: %d rows synced from rfn", len(rows))

    _apply_amount_corrections()


def _apply_amount_corrections() -> None:
    """Apply merchant+amount based category overrides to transactions_use."""
    with get_connection() as db:
        db.row_factory = sqlite3.Row
        rows = agentic.get_all_transactions_use_for_corrections(db)

        rows_updated = 0
        for row in rows:
            merchant_key = (row["merchant"] or "").lower()
            amount = row["amount"]

            matched = False
            for patterns, amounts, (cat_main, cat_second) in AMOUNT_CORRECTIONS:
                if (
                    any(p.lower() in merchant_key for p in patterns)
                    and amount in amounts
                ):
                    matched = True
                    if (
                        row["category_main"] != cat_main
                        or row["category_second"] != cat_second
                    ):
                        agentic.update_transactions_use_category_correction(
                            db,
                            category_main=cat_main,
                            category_second=cat_second,
                            id=row["id"],
                        )
                        rows_updated += 1
                    break

            if not matched:
                row_date = (row["date"] or "")[:10]
                for patterns, dates, (cat_main, cat_second) in DATE_CORRECTIONS:
                    if (
                        any(p.lower() in merchant_key for p in patterns)
                        and row_date in dates
                    ):
                        if (
                            row["category_main"] != cat_main
                            or row["category_second"] != cat_second
                        ):
                            agentic.update_transactions_use_category_correction(
                                db,
                                category_main=cat_main,
                                category_second=cat_second,
                                id=row["id"],
                            )
                            rows_updated += 1
                        matched = True
                        break

            if not matched:
                for (
                    patterns,
                    min_amount,
                    (cat_main, cat_second),
                ) in AMOUNT_THRESHOLD_CORRECTIONS:
                    if (
                        any(p.lower() in merchant_key for p in patterns)
                        and abs(amount) > min_amount
                    ):
                        if (
                            row["category_main"] != cat_main
                            or row["category_second"] != cat_second
                        ):
                            agentic.update_transactions_use_category_correction(
                                db,
                                category_main=cat_main,
                                category_second=cat_second,
                                id=row["id"],
                            )
                            rows_updated += 1
                        matched = True
                        break

            if not matched:
                row_year = int((row["date"] or "")[:4]) if row["date"] else None
                for patterns, year, (cat_main, cat_second) in YEAR_CORRECTIONS:
                    if (
                        any(p.lower() in merchant_key for p in patterns)
                        and row_year == year
                    ):
                        if (
                            row["category_main"] != cat_main
                            or row["category_second"] != cat_second
                        ):
                            agentic.update_transactions_use_category_correction(
                                db,
                                category_main=cat_main,
                                category_second=cat_second,
                                id=row["id"],
                            )
                            rows_updated += 1
                        break

        db.commit()

    logger.debug("transactions_use: %d rows corrected by amount", rows_updated)

    _apply_shared_housing_roommate_offset()


def _apply_shared_housing_roommate_offset() -> None:
    """Subtract roommate's share from Shared housing rent and remove the matching income row.

    Idempotent: resets Shared housing amounts from transactions_rfn before applying, so
    repeated runs always produce the same result even if income rows were
    re-inserted by a previous run_transactions_use() call.
    """
    if not HOUSING_RENT_2:
        return

    # dynamic LIKE clause from config list: cannot be expressed as static aiosql SQL
    like_clauses = " OR ".join(
        f"lower(tu.merchant) LIKE '%{p.lower()}%'" for p in HOUSING_RENT_2
    )

    with get_connection() as db:
        db.row_factory = sqlite3.Row

        # dynamic LIKE clause from config list: cannot be expressed as static aiosql SQL
        db.execute(
            f"""
            UPDATE transactions_use
            SET amount = (
                SELECT t.amount
                FROM transactions_rfn t
                WHERE t.reference = transactions_use.reference
            )
            WHERE {like_clauses.replace("tu.", "")}
            """
        )

        shared_housing_rows = db.execute(
            f"""
            SELECT tu.id, tu.date, tu.amount
            FROM transactions_use tu
            WHERE ({like_clauses})
              AND tu.category_main = 'Housing'
              AND tu.category_second = 'Rent'
            ORDER BY tu.date
            """
        ).fetchall()

        # First Shared housing row per month
        shared_housing_by_month: dict[str, tuple[int, float]] = {}
        for row in shared_housing_rows:
            month_key = row["date"][:7]
            if month_key not in shared_housing_by_month:
                shared_housing_by_month[month_key] = (
                    int(row["id"]),
                    float(row["amount"]),
                )

        income_rows = agentic.get_transactions_use_income_by_amount(
            db, amount=HOUSING_RENT_2_ROOMMATE_OFFSET
        )

        rows_adjusted = 0
        ids_to_delete: list[int] = []
        for income in income_rows:
            month_key = income["date"][:7]
            if month_key in shared_housing_by_month:
                shared_housing_id, shared_housing_amount = shared_housing_by_month[
                    month_key
                ]
                agentic.update_transactions_use_amount(
                    db,
                    amount=shared_housing_amount - HOUSING_RENT_2_ROOMMATE_OFFSET,
                    id=shared_housing_id,
                )
                ids_to_delete.append(income["id"])
                rows_adjusted += 1

        for income_id in ids_to_delete:
            agentic.delete_transactions_use_by_id(db, id=income_id)

        db.commit()

    logger.debug(
        "transactions_use: %d Shared housing roommate offsets applied", rows_adjusted
    )


def _backfill_balance_chf_use() -> None:
    """Propagate balance_chf from transactions_rfn into existing transactions_use rows."""
    with get_connection() as db:
        transactions.backfill_transactions_use_balance_chf(db)
        db.commit()

    logger.debug("transactions_use: balance_chf backfilled from transactions_rfn")
