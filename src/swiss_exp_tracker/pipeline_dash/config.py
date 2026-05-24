from __future__ import annotations


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
