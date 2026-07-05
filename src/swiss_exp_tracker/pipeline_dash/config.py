from __future__ import annotations

# Debit-account sources that carry a running balance (balance_chf).
# Used to derive the current-balance KPI and the balance-over-time chart.
BALANCE_SOURCE_TYPES: tuple[str, ...] = ("ZKB_DEBIT", "UBS_DEBIT")

# Substring → canonical brand name (applied in order; first match wins)
GROCERY_MERCHANT_NORMALIZE: list[tuple[str, str]] = [
    ("migrolino", "Migrolino"),
    ("migros", "Migros"),
    ("avec", "Avec"),
    ("bahnhofkiosk", "K-Kiosk"),
    ("kkiosk", "K-Kiosk"),
    ("k kiosk", "K-Kiosk"),
    ("valora", "K-Kiosk"),
    ("coop", "Coop"),
    ("lidl", "Lidl"),
    ("aldi", "Aldi"),
    ("denner", "Denner"),
    ("spar", "Spar"),
]

GROCERY_MERCHANTS_TRACKED: list[str] = [
    "Coop",
    "Migros",
    "Lidl",
    "Aldi",
    "Denner",
    "Avec",
    "Migrolino",
    "K-Kiosk",
    "Spar",
]

TRANSPORT_MAIN_CATEGORIES: list[str] = ["Car", "Transport"]

# Exclude car purchases from transport analytics
TRANSPORT_EXCLUDE_SECOND: list[str] = ["Purchase"]

# Exclude large one-off car costs from the heatmap (they distort the regular commuting signal)
TRANSPORT_HEATMAP_EXCLUDE_SECOND: list[str] = ["Service & Repair"]

# Exclude non-expenses from all dashboard analytics.
# Each entry: (category_main, category_second, merchant_substring_or_None)
# merchant_substring_or_None: case-insensitive substring match on the merchant
# column; None means match all merchants.
GLOBAL_EXCLUDE: list[tuple[str, str, str | None]] = [
    ("Car", "Purchase", None),
    # Credit card bill payments (Viseca) — internal account settlement
    ("Payment Services", "Payment Fees", "Viseca"),
    # Inter-account money transfers
    ("Payment Services", "Money Transfer", None),
]

SPORT_EXCLUDE_SECOND: list[str] = [
    "Sports Facility",
    "Unknown",
    "Sports Administration",
    "Sports services",
]

# Exclude from net-balance and stats expense totals — money movement, not real spending
NET_BALANCE_EXPENSE_EXCLUDE_MAIN: list[str] = [
    "Investing",
    "Salary",
]

# Minimum transactions in the last month for a category to qualify as Top Category;
# below this it is skipped in favour of the next-highest-spend eligible category.
TOP_CATEGORY_MIN_TRANSACTIONS: int = 4

TOP_EXPENSES_EXCLUDE_MAIN: list[str] = [
    "Government",
    "Finance",
    "Friend",
    "Housing",
    "Financial Services",
    "Healthcare",
    "Investing",
    "Insurance",
    "Payment Services",
]

# Ordered major categories for the balance-sheet category-spend table.
# Each entry: (display_label, category_main, category_second_or_None).
# When category_second_or_None is not None the builder also filters by category_second.
BALANCE_SHEET_MAJOR_CATEGORIES: list[tuple[str, str, str | None]] = [
    ("Rent", "Housing", "Rent"),
    ("Groceries", "Groceries", None),
    ("Car", "Car", None),
    ("Transport", "Transport", None),
    ("Travel", "Travel", None),
    ("Sport", "Sport", None),
    ("Restaurant", "Restaurant", None),
    ("Retail", "Retail", None),
    ("Healthcare", "Healthcare", None),
]
