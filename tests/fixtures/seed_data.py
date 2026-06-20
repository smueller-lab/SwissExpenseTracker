from __future__ import annotations

from tests.fixtures.models import GroceryUseFixture
from tests.fixtures.models import UseTransactionFixture

# Year ranges covered: 2022 (12 months), 2023 (12 months), 2024 (6 months = Jan-Jun)
_YEAR_RANGES: list[tuple[int, int]] = [(2022, 12), (2023, 12), (2024, 6)]

# Per-year extras placed in specific months for categorical coverage.
# Tuple: (month, category_main, category_second, merchant, amount, source_type)
_YEARLY_EXTRAS: list[tuple[int, str, str, str, float, str]] = [
    # Feb — Investing (balance-sheet invested column)
    (2, "Investing", "Brokerage", "Swissquote", 500.0, "ZKB_DEBIT"),
    # Mar — Car/Fuel (dash_car + dash_transport; NOT Purchase so not GLOBAL_EXCLUDE'd)
    (3, "Car", "Fuel", "Shell", 80.0, "ZKB_DEBIT"),
    # Apr — Sport/Golf, amount=100 satisfies 70<=amount<=160 for dash_sport_activities
    (4, "Sport", "Golf", "Golf Club Zürich", 100.0, "ZKB_DEBIT"),
    # May — Retail/Clothing (dash_retail)
    (5, "Retail", "Clothing", "Zara", 120.0, "VISECA_CREDIT"),
    # Jun — Travel/Flight (dash_vacation)
    (6, "Travel", "Flight", "Swiss Air", 350.0, "VISECA_CREDIT"),
]


def make_seed_transactions() -> list[UseTransactionFixture]:
    """Return deterministic UseTransactionFixture rows spanning 2022-01 to 2024-06, covering every category path (Salary/INCOME, Groceries, Housing, Restaurant, Transport, Travel, Car, Sport, Retail, Investing) with unique references and non-null balance_chf on all ZKB_DEBIT rows."""
    rows: list[UseTransactionFixture] = []
    tid = 1

    for year, n_months in _YEAR_RANGES:
        for month in range(1, n_months + 1):
            date = f"{year}-{month:02d}-15"
            # Deterministic balance that decreases slightly each iteration
            base_bal = 8000.0 - (tid % 50) * 20.0

            # Salary INCOME — gates get_IncomeExpenseMonthly (salary_months filter)
            rows.append(
                UseTransactionFixture(
                    transaction_id=tid,
                    source_type="ZKB_DEBIT",
                    date=date,
                    amount=7000.0,
                    transaction_type="INCOME",
                    currency="CHF",
                    reference=f"REF-{tid:05d}",
                    merchant="Employer AG",
                    category_main="Salary",
                    category_second=None,
                    city="Zurich",
                    balance_chf=base_bal + 7000.0,
                )
            )
            tid += 1

            # Groceries EXPENSE — merchant "Coop" is in GROCERY_MERCHANTS_TRACKED
            rows.append(
                UseTransactionFixture(
                    transaction_id=tid,
                    source_type="ZKB_DEBIT",
                    date=date,
                    amount=200.0,
                    transaction_type="EXPENSE",
                    currency="CHF",
                    reference=f"REF-{tid:05d}",
                    merchant="Coop",
                    category_main="Groceries",
                    category_second=None,
                    city="Zurich",
                    balance_chf=base_bal + 4800.0,
                )
            )
            tid += 1

            # Housing/Rent EXPENSE — balance-sheet Rent label
            rows.append(
                UseTransactionFixture(
                    transaction_id=tid,
                    source_type="ZKB_DEBIT",
                    date=date,
                    amount=1800.0,
                    transaction_type="EXPENSE",
                    currency="CHF",
                    reference=f"REF-{tid:05d}",
                    merchant="Landlord AG",
                    category_main="Housing",
                    category_second="Rent",
                    city="Zurich",
                    balance_chf=base_bal + 3000.0,
                )
            )
            tid += 1

            # Restaurant EXPENSE — dash_food; category_second drives subcategory breakdown
            rows.append(
                UseTransactionFixture(
                    transaction_id=tid,
                    source_type="VISECA_CREDIT",
                    date=date,
                    amount=25.0,
                    transaction_type="EXPENSE",
                    currency="CHF",
                    reference=f"REF-{tid:05d}",
                    merchant="Burger King",
                    category_main="Restaurant",
                    category_second="Fast Food",
                    city="Zurich",
                    balance_chf=None,
                )
            )
            tid += 1

            # Transport/Public Transport EXPENSE — dash_transport + dash_transport_heatmap
            # "Public Transport" is not in TRANSPORT_HEATMAP_EXCLUDE_SECOND
            rows.append(
                UseTransactionFixture(
                    transaction_id=tid,
                    source_type="ZKB_DEBIT",
                    date=date,
                    amount=80.0,
                    transaction_type="EXPENSE",
                    currency="CHF",
                    reference=f"REF-{tid:05d}",
                    merchant="SBB",
                    category_main="Transport",
                    category_second="Public Transport",
                    city="Zurich",
                    balance_chf=base_bal + 2920.0,
                )
            )
            tid += 1

    # Per-year categorical extras
    for year, _ in _YEAR_RANGES:
        for month, cat_main, cat_second, merchant, amount, src_type in _YEARLY_EXTRAS:
            date = f"{year}-{month:02d}-20"
            rows.append(
                UseTransactionFixture(
                    transaction_id=tid,
                    source_type=src_type,
                    date=date,
                    amount=amount,
                    transaction_type="EXPENSE",
                    currency="CHF",
                    reference=f"REF-{tid:05d}",
                    merchant=merchant,
                    category_main=cat_main,
                    category_second=cat_second,
                    city="Zurich" if src_type == "ZKB_DEBIT" else None,
                    balance_chf=4000.0 if src_type == "ZKB_DEBIT" else None,
                )
            )
            tid += 1

    return rows


def make_seed_groceries() -> list[GroceryUseFixture]:
    """Return deterministic GroceryUseFixture rows covering three grocery categories across distinct years so dash_groceries_cat, dash_groceries_health, and dash_groceries_top_articles are all non-empty."""
    return [
        GroceryUseFixture(
            rfn_id=1,
            date="2022-03-15",
            time="10:00:00",
            location="Coop Zürich",
            article="Apples",
            unit="kg",
            quantity=1.5,
            price_chf=2.50,
            discount_chf=0.0,
            category_main="Fresh Produce",
            category_detail="Fruit",
        ),
        GroceryUseFixture(
            rfn_id=2,
            date="2023-05-15",
            time="14:30:00",
            location="Migros Zürich",
            article="Whole Milk",
            unit="L",
            quantity=2.0,
            price_chf=1.80,
            discount_chf=0.0,
            category_main="Dairy & Eggs",
            category_detail="Milk",
        ),
        GroceryUseFixture(
            rfn_id=3,
            date="2024-02-15",
            time="09:15:00",
            location="Coop Zürich",
            article="Chicken Breast",
            unit="kg",
            quantity=0.5,
            price_chf=8.90,
            discount_chf=0.50,
            category_main="Meat & Fish",
            category_detail="Poultry",
        ),
    ]
