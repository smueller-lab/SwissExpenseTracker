from __future__ import annotations

from enum import Enum


class SourceType(Enum):
    ZKB_DEBIT = "ZKB_DEBIT"
    VISECA = "VISECA"
    REVOLUT = "REVOLUT"
    SWISSQUOTE = "SWISSQUOTE"
    MIGROS_GROCERY = "MIGROS_GROCERY"
    UBS_DEBIT = "UBS_DEBIT"
    UBS_CREDIT = "UBS_CREDIT"


# Each tuple is (debit/expense side, credit/settlement side) for a credit card pair.
CREDIT_CARD_SOURCE_PAIRS: list[tuple[SourceType, SourceType]] = [
    (SourceType.ZKB_DEBIT, SourceType.VISECA),
    (SourceType.UBS_DEBIT, SourceType.UBS_CREDIT),
]


def get_credit_card_source_pairs() -> list[tuple[SourceType, SourceType]]:
    """Return all (debit, credit) SourceType pairs representing credit card settlement links."""
    return CREDIT_CARD_SOURCE_PAIRS
