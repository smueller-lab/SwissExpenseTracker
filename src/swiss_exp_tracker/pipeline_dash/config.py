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

# Exclude capital expenditures from all dashboard analytics (car purchase, etc.)
GLOBAL_EXCLUDE: list[tuple[str, str]] = [
    ("Car", "Purchase"),
]

SPORT_EXCLUDE_SECOND: list[str] = [
    "Sports Facility",
    "Unknown",
    "Sports Administration",
    "Sports services",
]

TOP_EXPENSES_EXCLUDE_MAIN: list[str] = [
    "Government",
    "Finance",
    "Friend",
    "Housing",
    "Financial Services",
    "Healthcare",
    "Investing",
    "Payment Services",
]
