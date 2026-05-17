from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dash.dash_table import FormatTemplate


DB_PATH = Path(__file__).resolve().parent.parent / "database" / "transactions.db"


@dataclass
class config:
    # dTick
    vk_dTick_Grocery = {"Monthly": 100, "Yearly": 1000, "Visit": 20}
    vk_dTick_Food = {"Monthly": 200, "Yearly": 1000, "Visit": 20}
    dTick_Balance: int = 5000
    dTick_Pct: int = 20
    dTick_Vacation: int = 1000
    dTick_Transport: int = 1000
    vk_dTick_Sport = {"Monthly": 500, "Yearly": 1000}

    # npixel
    vk_npixel_Food = {"Monthly": 50, "Yearly": 40, "Visit": 50}
    vk_npixel_Grocery = {"Monthly": 50, "Yearly": 80, "Visit": 50}
    npixel_Balance: int = 80
    npixel_Pct: int = 80
    npixel_Vacation: int = 80
    npixel_Transport: int = 80
    vk_npixel_Sport = {"Monthly": 100, "Yearly": 80}

    # default ry_Axis
    ry_Axis_Pct = [0, 100]


@dataclass
class VIS:
    vk_Variable_show = {
        "amount_CHF": "Amount [CHF]",
        "category_main": "Main category",
        "category_second": "Second category",
        "MerchantPlace": "Location",
        "expense": "Expense",
        "income": "Income",
    }

    vk_format_col = {
        "Amount [CHF]": {
            "type": "numeric",
            "format": FormatTemplate.money(2).symbol(""),  # type: ignore[no-untyped-call]
        },
        "Expense": {"type": "numeric", "format": FormatTemplate.money(2).symbol("")},  # type: ignore[no-untyped-call]
        "Income": {"type": "numeric", "format": FormatTemplate.money(2).symbol("")},  # type: ignore[no-untyped-call]
        "NetBalance": {"type": "numeric", "format": FormatTemplate.money(2).symbol("")},  # type: ignore[no-untyped-call]
    }

    vk_GroceryStore_col = {
        "Coop": "#45C7F6",
        "Migros": "#E38A04",
        "Lidl": "#FAF263",
        "Aldi": "#FA6363",
        "Denner": "#D052E9",
        "Avec": "#17C528",
        "Migrolino": "#7563FA",
        "K-Kiosk": "#FF8C00",
        "Spar": "#2ECC71",
    }

    vk_Food_col = {
        "Groceries": "#45C7F6",
        "Supermarket": "#45C7F6",
        "Dining": "#E38A04",
        "Restaurant": "#E38A04",
        "Cafe": "#17C528",
        "Bakery": "#7563FA",
        "Bar": "#FA6363",
        "Cafeteria": "#FAF263",
        "Fast Food": "#C0392B",
        "Food Delivery": "#FF8C42",
        "Drinks": "#3498DB",
    }

    vk_Sport_col = {
        "Golf": "#4580F6",
        "Running": "#2ECC71",
        "Climbing Gym": "#27AE60",
        "Swimming Pool": "#1ABC9C",
        "Swimming": "#1ABC9C",
        "Tennis": "#F39C12",
        "Padel": "#E67E22",
        "Fitness Center": "#9B59B6",
        "Fitness": "#9B59B6",
        "Yoga Studio": "#8E44AD",
        "Football Club": "#E74C3C",
        "Football": "#E74C3C",
        "Stadium": "#C0392B",
        "University Sport": "#F1C40F",
        "Retail": "#BBCFE2",
        "Ticketing": "#7F8C8D",
        "Event Organizer": "#95A5A6",
        "Ice Hockey": "#1A5276",
        "Skiing": "#5DADE2",
        "Events & Concerts": "#D4AC0D",
        "Sports": "#95A5A6",
    }

    s_Merchant_Grocery = [
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
    s_Category_Food = [
        "Dining",
        "Groceries",
        "Cafe",
        "Bakery",
        "Bar",
        "Fast Food",
        "Food Delivery",
        "Drinks",
    ]
    s_Col_Text = [
        "Date",
        "Month",
        "Merchant",
        "Main category",
        "Second category",
        "Location",
    ]
