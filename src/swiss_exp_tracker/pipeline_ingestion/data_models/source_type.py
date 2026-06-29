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
