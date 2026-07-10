from __future__ import annotations

from datetime import date

import pytest

from pydantic import ValidationError

from swiss_exp_tracker.pipeline_ingestion.data_models.assets import AssetType
from swiss_exp_tracker.pipeline_ingestion.data_models.assets import SwissquotePositions
from swiss_exp_tracker.pipeline_ingestion.data_models.assets import (
    SwissquotePositionsRaw,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import Currency


def _make_position_raw(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "snapshot_date": date(2026, 1, 15),
        "asset_type": AssetType.SHARE,
        "symbol": "NESN",
        "quantity": 10.0,
        "opening_price": 100.0,
        "total_value": 1050.0,
        "day_change": 5.0,
        "day_difference": 50.0,
        "price": 105.0,
        "currency": Currency.CHF,
        "win_loss_nominal_value": 50.0,
        "win_loss_percentage": 5.0,
        "total_value_chf": 1050.0,
        "position_percentage": 0.25,
    }
    base.update(overrides)
    return base


def _make_positions(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "source_file": "swissquote_2026-01-15.csv",
        "snapshot_date": date(2026, 1, 15),
        "positions": [_make_position_raw()],
    }
    base.update(overrides)
    return base


def test_asset_type_values() -> None:
    assert AssetType.SHARE.value == "SHARE"
    assert AssetType.ETF.value == "ETF"


def test_swissquote_position_raw_parses_valid_row() -> None:
    model = SwissquotePositionsRaw.model_validate(_make_position_raw())
    assert model.symbol == "NESN"
    assert model.asset_type == AssetType.SHARE
    assert model.currency == Currency.CHF
    assert model.quantity == pytest.approx(10.0)


def test_swissquote_position_raw_allows_none_optional_fields() -> None:
    model = SwissquotePositionsRaw.model_validate(
        _make_position_raw(
            asset_type=None,
            symbol=None,
            quantity=None,
            opening_price=None,
            total_value=None,
            day_change=None,
            day_difference=None,
            price=None,
            currency=None,
            win_loss_nominal_value=None,
            win_loss_percentage=None,
            total_value_chf=None,
            position_percentage=None,
        )
    )
    assert model.asset_type is None
    assert model.symbol is None
    assert model.currency is None


def test_swissquote_position_raw_requires_snapshot_date() -> None:
    row = _make_position_raw()
    del row["snapshot_date"]
    with pytest.raises(ValidationError):
        SwissquotePositionsRaw.model_validate(row)


def test_swissquote_position_raw_rejects_invalid_asset_type() -> None:
    with pytest.raises(ValidationError):
        SwissquotePositionsRaw.model_validate(_make_position_raw(asset_type="BOND"))


def test_swissquote_positions_parses_valid_payload() -> None:
    model = SwissquotePositions.model_validate(_make_positions())
    assert model.source_file == "swissquote_2026-01-15.csv"
    assert len(model.positions) == 1
    assert model.positions[0].symbol == "NESN"


def test_swissquote_positions_defaults_to_empty_list() -> None:
    payload = _make_positions()
    del payload["positions"]
    model = SwissquotePositions.model_validate(payload)
    assert model.positions == []


def test_swissquote_positions_roundtrip() -> None:
    model = SwissquotePositions.model_validate(_make_positions())
    dumped = model.model_dump(mode="python")
    revalidated = SwissquotePositions.model_validate(dumped)
    assert revalidated == model
