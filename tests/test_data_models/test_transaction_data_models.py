from __future__ import annotations

import csv
import random

from pathlib import Path

import pytest

from swiss_exp_tracker.pipeline_ingestion.adapters import RevolutAdapter
from swiss_exp_tracker.pipeline_ingestion.adapters import VisecaAdapter
from swiss_exp_tracker.pipeline_ingestion.adapters import ZKBDebitAdapter
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    RevolutTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import TransactionType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    UnifiedTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    VisecaTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import ZKBTransaction


TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"
UNIFIED_FIELDS = set(UnifiedTransaction.model_fields)
RANDOM_SEED = 42
SAMPLE_SIZE = 100


def _load_csv_rows(file_name: str) -> list[dict[str, str]]:
    with (TEST_DATA_DIR / file_name).open(encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def _sample_rows(file_name: str, sample_size: int, seed: int) -> list[dict[str, str]]:
    rows = _load_csv_rows(file_name)
    if len(rows) <= sample_size:
        return rows
    rng = random.Random(seed)
    return rng.sample(rows, sample_size)


def _assert_valid_unified_transaction(unified: UnifiedTransaction) -> None:
    validated_unified = UnifiedTransaction.model_validate(
        unified.model_dump(mode="python")
    )
    dumped = validated_unified.model_dump(mode="python")

    assert isinstance(validated_unified, UnifiedTransaction)
    assert set(dumped) == UNIFIED_FIELDS
    assert validated_unified.id is not None
    assert validated_unified.source is not None
    assert validated_unified.reference_id is not None
    assert validated_unified.amount is not None
    assert validated_unified.currency is not None
    assert validated_unified.transaction_type in set(TransactionType)
    assert validated_unified.source_file


@pytest.mark.parametrize("row", _sample_rows("zkb_test.csv", SAMPLE_SIZE, RANDOM_SEED))
def test_zkb_debit_to_unified_transactions(row: dict[str, str]) -> None:
    # model validation and conversion to unified transaction
    source_model = ZKBTransaction.model_validate(row)
    unified = ZKBDebitAdapter().to_unified(source_model, "zkb_test.csv")

    # validate unified model
    assert isinstance(source_model, ZKBTransaction)
    assert isinstance(unified, UnifiedTransaction)
    _assert_valid_unified_transaction(unified)


@pytest.mark.parametrize(
    "row", _sample_rows("viseca_test.csv", SAMPLE_SIZE, RANDOM_SEED)
)
def test_viseca_to_unified_transactions(row: dict[str, str]) -> None:
    # model validation and conversion to unified transaction
    source_model = VisecaTransaction.model_validate(row)
    unified = VisecaAdapter().to_unified(source_model, "viseca_test.csv")

    # validate unified model
    assert isinstance(source_model, VisecaTransaction)
    assert isinstance(unified, UnifiedTransaction)
    _assert_valid_unified_transaction(unified)


@pytest.mark.parametrize(
    "row", _sample_rows("account_statement_EUR_test.csv", SAMPLE_SIZE, RANDOM_SEED)
)
def test_revolut_to_unified_transactions(row: dict[str, str]) -> None:
    # model validation and conversion to unified transaction
    source_model = RevolutTransaction.model_validate(row)
    unified = RevolutAdapter().to_unified(
        source_model, "account_statement_EUR_test.csv"
    )

    # validate unified model
    assert isinstance(source_model, RevolutTransaction)
    assert isinstance(unified, UnifiedTransaction)
    _assert_valid_unified_transaction(unified)
