from __future__ import annotations

from pydantic import BaseModel
from pydantic import field_validator

# ── DetectedRules — written by auto_detect.py ────────────────────────────────


class SalaryDetected(BaseModel):
    employers: list[str] = []


class RentDetected(BaseModel):
    merchant: str
    amounts: list[float] = []

    @field_validator("amounts", mode="before")
    @classmethod
    def coerce_amounts(cls, value: object) -> list[float]:
        """Coerce YAML integer entries to float in the amounts list."""
        if not isinstance(value, list):
            return []
        return [float(v) for v in value]


class HousingDetected(BaseModel):
    rent: list[RentDetected] = []


class DetectedRules(BaseModel):
    salary: SalaryDetected = SalaryDetected()
    housing: HousingDetected = HousingDetected()


# ── UserConfig — written by the user in user_config.yaml ─────────────────────


class SalaryDonation(BaseModel):
    name: str


class SalaryUser(BaseModel):
    employers: list[str] = []
    employers_exclude: list[str] = []
    donations: list[SalaryDonation] = []


class RentUser(BaseModel):
    merchant: str
    amounts: list[float] | None = None

    @field_validator("amounts", mode="before")
    @classmethod
    def coerce_amounts(cls, value: object) -> list[float] | None:
        """Coerce YAML integer entries to float; treat empty list as None."""
        if value is None:
            return None
        if isinstance(value, list):
            return [float(v) for v in value] if value else None
        return None


class DepositRule(BaseModel):
    merchant: str
    min_amount: float


class SharedHousingRule(BaseModel):
    merchant: str
    roommate_offset: float


class HousingUser(BaseModel):
    rent: list[RentUser] = []
    rent_exclude: list[str] = []
    deposits: list[DepositRule] = []
    shared_housing: list[SharedHousingRule] = []


class InvestingTransfer(BaseModel):
    merchant: str
    min_amount: float | None = None
    dates: list[str] | None = None


class InvestingUser(BaseModel):
    transfers: list[InvestingTransfer] = []


class TravelAllInclusive(BaseModel):
    merchant: str
    year: int | None = None
    dates: list[str] | None = None

    @field_validator("year", mode="before")
    @classmethod
    def coerce_year(cls, value: object) -> int | None:
        """Coerce string or float year values to int; pass None through."""
        if value is None:
            return None
        if isinstance(value, (int, float, str)):
            return int(value)
        raise ValueError(f"Cannot coerce {value!r} to int for year field")


class TravelUser(BaseModel):
    all_inclusive: list[TravelAllInclusive] = []


class CustomRule(BaseModel):
    merchant: str
    category_main: str
    category_second: str
    exact_match: bool = False


class ReferenceIdCorrection(BaseModel):
    reference_id: str
    merchant: str
    category_main: str
    category_second: str
    city: str | None = None


class UserConfig(BaseModel):
    salary: SalaryUser = SalaryUser()
    housing: HousingUser = HousingUser()
    investing: InvestingUser = InvestingUser()
    travel: TravelUser = TravelUser()
    custom_rules: list[CustomRule] = []
    reference_id_corrections: list[ReferenceIdCorrection] = []


# ── MergedConfig — produced by loader, consumed by the correction engine ─────


class SalaryMerged(BaseModel):
    employers: list[str] = []
    donations: list[SalaryDonation] = []


class HousingMerged(BaseModel):
    rent: list[RentUser] = []
    deposits: list[DepositRule] = []
    shared_housing: list[SharedHousingRule] = []


class MergedConfig(BaseModel):
    salary: SalaryMerged = SalaryMerged()
    housing: HousingMerged = HousingMerged()
    investing: InvestingUser = InvestingUser()
    travel: TravelUser = TravelUser()
    custom_rules: list[CustomRule] = []
    reference_id_corrections: list[ReferenceIdCorrection] = []
