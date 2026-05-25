from __future__ import annotations

import re

from datetime import date as dt_date
from datetime import time as dt_time
from enum import StrEnum

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import computed_field
from pydantic import field_validator


class GroceryUnit(StrEnum):
    KG = "kg"
    QTY = "qty"


# Compiled patterns applied in order by normalize_article.
# 1. Count x size: "6*53g", "4x100g", "2x500ml"
_COUNT_SIZE_RE = re.compile(
    r"\d+\s*[x*\xd7]\s*\d+(\.\d+)?\s*(g|kg|mg|ml|cl|dl|l)\b",
    re.IGNORECASE,
)
# 2. Standalone weight/volume suffix: "330ml", "50cl", "100g", "1kg"
_UNIT_SUFFIX_RE = re.compile(
    r"\b\d+(\.\d+)?\s*(g|kg|mg|ml|cl|dl|l)\b",
    re.IGNORECASE,
)
# 3. Trailing standalone integer: "Brot 750"
_TRAILING_INT_RE = re.compile(r"\s+\d+$")


def normalize_article(article: str) -> str:
    """Strip size/unit suffixes from an article name for vector-store normalisation.

    "Haribo Goldbären 100g" -> "Haribo Goldbären"
    "Aproz Gazeifiee 50cl"  -> "Aproz Gazeifiee"
    "Eier Freiland 6*53g"   -> "Eier Freiland"
    """
    name = _COUNT_SIZE_RE.sub("", article)
    name = _UNIT_SUFFIX_RE.sub("", name)
    name = _TRAILING_INT_RE.sub("", name)
    return re.sub(r"\s+", " ", name).strip()


class GroceryItem(BaseModel):
    """Raw row from Migros receipt CSV. German CSV headers mapped via Field aliases."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    date: dt_date = Field(alias="Datum")
    time: dt_time = Field(alias="Zeit")
    location: str = Field(alias="Filiale")
    article: str = Field(alias="Artikel")
    quantity: float = Field(alias="Menge")
    discount: float = Field(alias="Aktion")
    price: float = Field(alias="Umsatz")

    @field_validator("date", mode="before")
    @classmethod
    def _parse_date(cls, value: object) -> object:
        if isinstance(value, str):
            from datetime import datetime

            for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
            msg = f"Cannot parse date: {value!r}"
            raise ValueError(msg)
        return value

    @field_validator("time", mode="before")
    @classmethod
    def _parse_time(cls, value: object) -> object:
        if isinstance(value, str):
            from datetime import datetime

            return datetime.strptime(value, "%H:%M:%S").time()
        return value

    @field_validator("quantity", "discount", "price", mode="before")
    @classmethod
    def _parse_float(cls, value: object) -> object:
        if isinstance(value, str):
            return float(value.replace(",", "."))
        return value

    @computed_field  # type: ignore[prop-decorator]
    @property
    def unit(self) -> GroceryUnit:
        abs_qty = abs(self.quantity)
        return GroceryUnit.KG if abs_qty != int(abs_qty) else GroceryUnit.QTY

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_bonus_row(self) -> bool:
        return self.price == 0 and self.discount == 0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_return_row(self) -> bool:
        return self.quantity < 0
