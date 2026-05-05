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

from swiss_exp_tracker.config import work_places
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategorySecond


oj = os.path.join

# ---------------------------------------------------------------------------
# Manual corrections
# Key   : matched_merchant (case-insensitive exact match)
# Value : (category_main, category_second, city_override or None)
# ---------------------------------------------------------------------------
# Exact-match corrections: matched_merchant (case-insensitive) -> (category_main, category_second, city_override)
CORRECTIONS: dict[str, tuple[str, str, str | None]] = {}

# Containment corrections: if any substring in the list is contained in matched_merchant,
# the correction is applied (case-insensitive).
CONTAINMENT_CORRECTIONS: list[tuple[list[str], tuple[str, str, str | None]]] = [
    (work_places, (CategoryMain.SALARY.value, CategorySecond.SALARY_MAIN.value, None)),
]

# SWITZERLAND FIRST
# Parkingpay App, Transport, Parking -> should be Car_Parking
# Liegenschaft Neumarkt, Housing Rent -> Idk
# Raststätte Knonauer Amt, Car Service Reapi -> should be Fueling

# Restaurant Parking is not possible
# add SPA to categories
# Any place that serve food should be restaurant except for Bar

# datsport, Swiss Ski, schweizer schwimmverband to Salary

# Car, car washing


def run_post_clean() -> None:
    """Read merchant_metadata_raw, apply corrections, write merchant_metadata_rfn.

    - Takes the latest raw row per ``zkb_reference`` (highest ``id``).
    - Rows already present in ``merchant_metadata_rfn`` (by ``zkb_reference``)
      are skipped so the function is safe to call repeatedly.
    """
    path_db = oj("./database", "transactions.db")

    with sqlite3.connect(path_db) as db:
        db.row_factory = sqlite3.Row

        # Ensure destination table exists
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS merchant_metadata_rfn (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                zkb_reference TEXT,
                matched_merchant TEXT NOT NULL,
                cache_hit INTEGER NOT NULL,
                similarity REAL,
                search_tool TEXT,
                category_main TEXT,
                category_second TEXT,
                city TEXT
            )
            """
        )

        # Latest raw row per zkb_reference, excluding already-processed ones
        raw_rows = db.execute(
            """
            SELECT r.*
            FROM merchant_metadata_raw r
            INNER JOIN (
                SELECT zkb_reference, MAX(id) AS max_id
                FROM merchant_metadata_raw
                GROUP BY zkb_reference
            ) latest ON r.id = latest.max_id
            WHERE r.zkb_reference NOT IN (
                SELECT zkb_reference
                FROM merchant_metadata_rfn
                WHERE zkb_reference IS NOT NULL
            )
            ORDER BY r.created_at DESC
            """
        ).fetchall()

        corrections_lookup = {k.lower(): v for k, v in CORRECTIONS.items()}
        containment_corrections = [
            ([s.lower() for s in patterns], correction)
            for patterns, correction in CONTAINMENT_CORRECTIONS
        ]

        rows_inserted = 0

        for row in raw_rows:
            merchant_key = (row["matched_merchant"] or "").lower()

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

            db.execute(
                """
                INSERT INTO merchant_metadata_rfn (
                    created_at, zkb_reference, matched_merchant,
                    cache_hit, similarity, search_tool,
                    category_main, category_second, city
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["created_at"],
                    row["zkb_reference"],
                    row["matched_merchant"],
                    row["cache_hit"],
                    row["similarity"],
                    row["search_tool"],
                    category_main,
                    category_second,
                    city,
                ),
            )
            rows_inserted += 1

        db.commit()

    tqdm.write(f"post_clean: {rows_inserted} rows written to merchant_metadata_rfn")
