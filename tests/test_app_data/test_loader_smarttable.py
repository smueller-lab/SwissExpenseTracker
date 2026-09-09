"""Tests for DataLoader.rename_merchant and DataLoader.recategorize_merchant.

Uses a fresh fixture DB + tmp user_config.yaml per test — never touches the
real transactions.db or the real user_config.yaml.
"""

from __future__ import annotations

import sqlite3

from pathlib import Path

import pytest

from pytest_mock import MockerFixture

from swiss_exp_tracker.app.data.loader import DataLoader
from swiss_exp_tracker.config.user_config_loader import load
from swiss_exp_tracker.pipeline_dash.pipeline import run_dashboard_pipeline
from tests.fixtures.db_builder import build_fixture_db
from tests.fixtures.seed_data import make_seed_groceries
from tests.fixtures.seed_data import make_seed_transactions


@pytest.fixture()
def writable_loader(tmp_path: Path) -> DataLoader:
    """Fresh DB + DataLoader with an isolated user_config.yaml, safe for edit methods."""
    db_path = tmp_path / "transactions.db"
    build_fixture_db(db_path, make_seed_transactions(), make_seed_groceries())
    run_dashboard_pipeline(db_path=db_path)
    user_config_path = tmp_path / "user_config.yaml"
    return DataLoader(db_path=db_path, user_config_path=user_config_path)


# ---------------------------------------------------------------------------
# rename_merchant
# ---------------------------------------------------------------------------


def test_rename_merchant_updates_only_targeted_row(writable_loader: DataLoader) -> None:
    """Renaming one Coop transaction leaves every other Coop transaction unchanged."""
    coop_rows = writable_loader.pdf_Master[
        writable_loader.pdf_Master["Merchant"] == "Coop"
    ]
    assert len(coop_rows) > 1
    target_reference = str(coop_rows.iloc[0]["reference"])

    writable_loader.rename_merchant(target_reference, "Coop Supermarket")

    updated = writable_loader.pdf_Master
    targeted = updated[updated["reference"] == target_reference]
    others = updated[
        (updated["Merchant"] == "Coop") & (updated["reference"] != target_reference)
    ]
    assert targeted.iloc[0]["Merchant"] == "Coop Supermarket"
    assert len(others) == len(coop_rows) - 1
    assert (others["Merchant"] == "Coop").all()


def test_rename_merchant_persists_reference_id_correction(
    writable_loader: DataLoader,
) -> None:
    """The rename is written to user_config.yaml as a reference_id_corrections entry."""
    coop_rows = writable_loader.pdf_Master[
        writable_loader.pdf_Master["Merchant"] == "Coop"
    ]
    target_reference = str(coop_rows.iloc[0]["reference"])
    original_category = str(coop_rows.iloc[0]["category_main"])

    writable_loader.rename_merchant(target_reference, "Coop Supermarket")

    cfg = load(user_config_path=writable_loader._user_config_path)
    matches = [
        c for c in cfg.reference_id_corrections if c.reference_id == target_reference
    ]
    assert len(matches) == 1
    assert matches[0].merchant == "Coop Supermarket"
    # Category must be preserved, not altered by a merchant-only rename.
    assert matches[0].category_main == original_category


def test_rename_merchant_rejects_empty_name(writable_loader: DataLoader) -> None:
    """An empty new merchant name raises ValueError and changes nothing."""
    coop_rows = writable_loader.pdf_Master[
        writable_loader.pdf_Master["Merchant"] == "Coop"
    ]
    target_reference = str(coop_rows.iloc[0]["reference"])

    with pytest.raises(ValueError):
        writable_loader.rename_merchant(target_reference, "   ")

    unchanged = writable_loader.pdf_Master[
        writable_loader.pdf_Master["reference"] == target_reference
    ]
    assert unchanged.iloc[0]["Merchant"] == "Coop"


def test_rename_merchant_unknown_reference_raises(writable_loader: DataLoader) -> None:
    """A reference with no matching transaction raises ValueError."""
    with pytest.raises(ValueError):
        writable_loader.rename_merchant("REF-DOES-NOT-EXIST", "New Name")


def test_rename_merchant_yaml_failure_still_commits_db_write(
    writable_loader: DataLoader, mocker: MockerFixture
) -> None:
    """If the YAML write fails, the DB update has already committed (per design.md Decision 3)."""
    coop_rows = writable_loader.pdf_Master[
        writable_loader.pdf_Master["Merchant"] == "Coop"
    ]
    target_reference = str(coop_rows.iloc[0]["reference"])
    mocker.patch(
        "swiss_exp_tracker.config.user_config_writer.upsert_reference_id_correction",
        side_effect=OSError("disk full"),
    )

    with pytest.raises(RuntimeError):
        writable_loader.rename_merchant(target_reference, "Coop Supermarket")

    with sqlite3.connect(str(writable_loader._db_path)) as con:
        row = con.execute(
            "SELECT merchant FROM transactions_use WHERE reference = ?",
            (target_reference,),
        ).fetchone()
    assert row[0] == "Coop Supermarket"


# ---------------------------------------------------------------------------
# recategorize_merchant
# ---------------------------------------------------------------------------


def test_recategorize_merchant_updates_every_matching_row(
    writable_loader: DataLoader,
) -> None:
    """Recategorizing Coop updates every Coop row's category, not just one."""
    coop_rows = writable_loader.pdf_Master[
        writable_loader.pdf_Master["Merchant"] == "Coop"
    ]
    n_coop = len(coop_rows)
    assert n_coop > 1

    writable_loader.recategorize_merchant("Coop", "Groceries", "Supermarket")

    updated_coop = writable_loader.pdf_Master[
        writable_loader.pdf_Master["Merchant"] == "Coop"
    ]
    assert len(updated_coop) == n_coop
    assert (updated_coop["category_main"] == "Groceries").all()
    assert (updated_coop["category_second"] == "Supermarket").all()


def test_recategorize_merchant_does_not_affect_other_merchants(
    writable_loader: DataLoader,
) -> None:
    """Recategorizing Coop leaves other merchants' categories untouched."""
    other_rows_before = writable_loader.pdf_Master[
        writable_loader.pdf_Master["Merchant"] == "Employer AG"
    ][["category_main", "category_second"]].copy()

    writable_loader.recategorize_merchant("Coop", "Groceries", "Supermarket")

    other_rows_after = writable_loader.pdf_Master[
        writable_loader.pdf_Master["Merchant"] == "Employer AG"
    ][["category_main", "category_second"]]
    assert other_rows_before.reset_index(drop=True).equals(
        other_rows_after.reset_index(drop=True)
    )


def test_recategorize_merchant_persists_custom_rule(
    writable_loader: DataLoader,
) -> None:
    """The recategorization is written to user_config.yaml as an exact-match custom_rules entry."""
    writable_loader.recategorize_merchant("Coop", "Groceries", "Supermarket")

    cfg = load(user_config_path=writable_loader._user_config_path)
    matches = [r for r in cfg.custom_rules if r.merchant.lower() == "coop"]
    assert len(matches) == 1
    assert matches[0].category_main == "Groceries"
    assert matches[0].category_second == "Supermarket"
    assert matches[0].exact_match is True


def test_recategorize_merchant_reedit_replaces_rule_not_duplicates(
    writable_loader: DataLoader,
) -> None:
    """Recategorizing the same merchant twice replaces the custom_rules entry, not duplicates it."""
    writable_loader.recategorize_merchant("Coop", "Groceries", "Supermarket")
    writable_loader.recategorize_merchant("Coop", "Retail", "Convenience")

    cfg = load(user_config_path=writable_loader._user_config_path)
    matches = [r for r in cfg.custom_rules if r.merchant.lower() == "coop"]
    assert len(matches) == 1
    assert matches[0].category_main == "Retail"
    assert matches[0].category_second == "Convenience"


@pytest.mark.parametrize(
    ("merchant", "category_main", "category_second"),
    [
        ("", "Groceries", "Supermarket"),
        ("Coop", "", "Supermarket"),
    ],
)
def test_recategorize_merchant_rejects_empty_fields(
    writable_loader: DataLoader, merchant: str, category_main: str, category_second: str
) -> None:
    """An empty merchant or category raises ValueError."""
    with pytest.raises(ValueError):
        writable_loader.recategorize_merchant(merchant, category_main, category_second)


def test_recategorize_merchant_allows_empty_subcategory(
    writable_loader: DataLoader,
) -> None:
    """An empty category_second is allowed (represents "no subcategory") and stores NULL,
    matching how transactions_use.category_second is nullable in the DB."""
    writable_loader.recategorize_merchant("Coop", "Groceries", "")

    updated_coop = writable_loader.pdf_Master[
        writable_loader.pdf_Master["Merchant"] == "Coop"
    ]
    assert (updated_coop["category_main"] == "Groceries").all()
    assert updated_coop["category_second"].isna().all()

    with sqlite3.connect(str(writable_loader._db_path)) as con:
        rows = con.execute(
            "SELECT category_second FROM transactions_use WHERE merchant = 'Coop'"
        ).fetchall()
    assert all(r[0] is None for r in rows)

    cfg = load(user_config_path=writable_loader._user_config_path)
    matches = [r for r in cfg.custom_rules if r.merchant.lower() == "coop"]
    assert len(matches) == 1
    assert matches[0].category_second == ""
