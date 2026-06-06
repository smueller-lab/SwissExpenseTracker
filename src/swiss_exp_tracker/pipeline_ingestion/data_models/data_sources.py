from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

from pydantic import BaseModel

from swiss_exp_tracker.pipeline_ingestion.adapters.adapters import RevolutAdapter
from swiss_exp_tracker.pipeline_ingestion.adapters.adapters import VisecaAdapter
from swiss_exp_tracker.pipeline_ingestion.adapters.adapters import ZKBDebitAdapter
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    RevolutTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import (
    VisecaTransaction,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import ZKBTransaction

if TYPE_CHECKING:
    from swiss_exp_tracker.pipeline_ingestion.adapters.adapters import BaseAdapter


SOURCE_MODEL_MAP: dict[SourceType, type[BaseModel]] = {
    SourceType.ZKB_DEBIT: ZKBTransaction,
    SourceType.VISECA: VisecaTransaction,
    SourceType.REVOLUT: RevolutTransaction,
}


def get_source_adapter_map() -> dict[SourceType, BaseAdapter[Any]]:

    return {
        SourceType.ZKB_DEBIT: ZKBDebitAdapter(),
        SourceType.VISECA: VisecaAdapter(),
        SourceType.REVOLUT: RevolutAdapter(),
    }
