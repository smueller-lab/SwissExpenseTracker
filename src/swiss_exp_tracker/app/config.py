from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

from dash.dash_table import FormatTemplate

DB_PATH = Path(__file__).resolve().parents[3] / "database" / "transactions.db"


@dataclass
class config:
    # dTick
    dTick_Pct: int = 20

    # npixel — pixels per gridline step. With the fixed-height formula
    # (PLOT_TARGET_STEPS x npixel + margins) this pins every value-axis plot to a
    # uniform ~460 px, matching height_Sport. Keep these aligned; do not raise
    # above ~50 or the plot grows tall again.
    vk_npixel_Food = {"Monthly": 50, "Yearly": 50, "Visit": 50}
    vk_npixel_Grocery = {"Monthly": 50, "Yearly": 50, "Visit": 50}
    vk_npixel_GroceryCat = {"Monthly": 50, "Yearly": 50}
    npixel_IncomeExpense: int = 50
    npixel_Pct: int = 50
    npixel_Vacation: int = 50
    npixel_Transport: int = 50
    npixel_Car: int = 60
    npixel_Sport: int = 60
    npixel_Retail: int = 60

    # default ry_Axis
    ry_Axis_Pct = [0, 100]

    # fixed height shared by all sport page charts (expenses + activities)
    height_Sport: int = 450

    # fixed heights for charts that do not use the adaptive-height formula
    height_Food: int = 600
    height_HealthIndex: int = 300

    # category grouping — categories outside the top-N (by total spend) are folded
    # into a single "Other" bucket to keep legends and donuts readable. Stacked bar
    # charts keep more series (wider legends fit) than donuts.
    category_top_n: int = 10  # donuts
    category_top_n_bar: int = 15  # stacked bar charts
    category_other_label: str = "Other"

    # donut chart constants (see .claude/rules/plots.md)
    donut_hole: float = 0.4
    donut_pull: float = 0.02
    donut_min_pct: float = 1.0
    donut_domain = {"x": [0.0, 0.9], "y": [0.0, 1.0]}
    donut_hovertemplate: str = (
        "%{label}<br>%{value:,.2f} CHF (%{percent})<extra></extra>"
    )

    # heatmap layout (see .claude/rules/plots.md)
    heatmap_row_px: int = 35
    heatmap_height_pad: int = 80
    heatmap_min_height: int = 220
    corr_min_height: int = 300
    corr_max_height: int = 1200
    corr_text_threshold: float = 0.5
    corr_zmin: int = -1
    corr_zmax: int = 1
    corr_label_margin = {"l": 80, "b": 70}  # offsets added to base template margins

    # grocery health index - score thresholds (0-100) and band opacity
    health_score_max: int = 100
    health_score_good: int = 70
    health_score_ok: int = 40
    health_band_opacity: float = 0.12

    # number of top categories shown in the vacation boxplot
    vacation_top_n: int = 4

    # budget / forecast page
    npixel_Budget: int = 50
    budget_default_categories: list[str] = field(
        default_factory=lambda: [
            "Travel",
            "Restaurant",
            "Groceries",
            "Transport",
            "Retail",
        ]
    )
    # Fixed resolution for the budget/forecast page (the UI selector was removed).
    forecast_freq_default: str = "W"
    # Shrinkage constant k for the year-end blend: forecast = w*pace + (1-w)*history,
    # with w = elapsed_year_fraction / (elapsed_year_fraction + k). Current-year pace is
    # the main driver; history anchors more of the blend for longer. Lower k trusts the
    # current year even sooner; raise it to lean on the (calmer, averaged) historical
    # level for longer when current-year pace is the volatile side of the blend (e.g. a
    # spiky category's pace still runs high after flattening one-off months).
    forecast_shrinkage_k: float = 0.3
    # Prior years of monthly history used to detect lumpy categories and their median monthly
    # rate; the current year's completed months are always included on top of this.
    forecast_lumpy_lookback_years: int = 2

    # in-chart text-label fonts
    font_no_data = {"color": "white", "size": 16}
    textfont_label = {"size": 12}  # donut + line-chart labels
    textfont_label_dense = {"size": 11}  # drill-down donut with many slices
    textfont_heatmap = {"size": 10}  # heatmap cell annotations
    textfont_heatmap_corr = {"color": "white", "size": 14}  # correlation cells


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
        "Ice Cream": "#F8BBD0",
        "Catering": "#A29BFE",
    }

    vk_Retail_col = {
        "Sport": "#4580F6",
        "Home Goods": "#E67E22",
        "Electronics": "#2ECC71",
        "Department Store": "#9B59B6",
        "Clothing": "#E74C3C",
        "Specialty Food": "#F1C40F",
        "Book Store": "#1ABC9C",
        "Drug Store": "#3498DB",
        "Bike": "#27AE60",
        "Furniture": "#D35400",
        "Shoes": "#C0392B",
        "Photography": "#8E44AD",
        "Garden": "#229954",
        "Music Sheets & Scores": "#F8C471",
        "Art Supplies": "#A569BD",
        "Online Shopping": "#5DADE2",
        "Cafe": "#17C528",
        "Dining": "#E38A04",
        "Drinks": "#3498DB",
        "Other Goods": "#95A5A6",
    }

    vk_Car_col = {
        "Fuel": "#F39C12",
        "Parking": "#3498DB",
        "Service & Repair": "#E74C3C",
        "Car Rental": "#9B59B6",
        "Insurance": "#2ECC71",
        "Wash": "#1ABC9C",
        "Car Wash": "#1ABC9C",
    }

    # shared categories must use the exact same hex as vk_Car_col
    vk_Transport_col = {
        "Fuel": "#F39C12",
        "Parking": "#3498DB",
        "Service & Repair": "#E74C3C",
        "Car Rental": "#9B59B6",
        "Wash": "#1ABC9C",
        "Flight": "#19D3F3",
        "Train": "#E38A04",
        "Bus": "#2ECC71",
        "Taxi": "#FAF263",
        "Public Transport": "#5DADE2",
        "Metro": "#8E44AD",
        "Ferry": "#1F618D",
        "Travel": "#E67E22",
        "Airport Services": "#F1C40F",
        "Bike Rental": "#27AE60",
        "Cable Car": "#148F77",
        "Toll Payment": "#7F8C8D",
        "Logistics": "#BDC3C7",
        "General Warehousing": "#95A5A6",
    }

    vk_Sport_col = {
        "Golf": "#4580F6",
        "Running": "#2ECC71",
        "Climbing Gym": "#27AE60",
        "Swimming Pool": "#1ABC9C",
        "Swimming": "#1ABC9C",
        "Tennis": "#F39C12",
        "Padel": "#E74C3C",
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

    vk_IncomeExpense_col = {
        "Expenses": "#FA6363",
        "Income": "#4580F6",
    }

    vk_GroceryCat_col = {
        "Fresh Produce": "#4caf50",
        "Beverages": "#4fc3f7",
        "Bakery & Bread": "#ff9800",
        "Snacks & Sweets": "#e91e63",
        "Dairy & Eggs": "#9c27b0",
        "Ready Meals": "#f44336",
        "Meat & Fish": "#ff5722",
        "Canned & Preserved": "#607d8b",
        "Frozen Foods": "#00bcd4",
        "Pasta & Grains": "#8bc34a",
        "Baking": "#795548",
        "Personal & Household": "#9e9e9e",
        "Other": "#546e7a",
    }

    # generic fallback colour for categories with no entry in a colour map
    fallback_col = "#95A5A6"

    # Distinct cycling palette for categories with no named colour map entry, so a
    # multi-category chart never collapses several categories onto the grey fallback.
    vk_cycling_col = [
        "#19D3F3",
        "#F368E0",
        "#FFD93D",
        "#6C5CE7",
        "#00CEC9",
        "#FF7675",
        "#55EFC4",
        "#FAB1A0",
        "#74B9FF",
        "#E17055",
        "#A29BFE",
        "#FD79A8",
        "#81ECEC",
        "#FFEAA7",
        "#00B894",
        "#E84393",
    ]

    # grocery health-index band colours (score thresholds live in `config`)
    vk_HealthBand_col = {
        "good": "#4caf50",
        "ok": "#ff9800",
        "bad": "#ef5350",
    }
    health_line_col = "#4fc3f7"

    # Heatmap colorscales — one per heatmap type (see .claude/rules/plots.md)
    vk_heatmap_colorscale = {
        "financial": "RdYlGn_r",
        "category_spend": "Greens",
        "correlation": [[0.0, "#b2182b"], [0.5, "#f7f7f7"], [1.0, "#2166ac"]],
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
        "Cafe",
        "Bakery",
        "Bar",
        "Fast Food",
        "Food Delivery",
        "Drinks",
        "Ice Cream",
        "Catering",
    ]
    s_Col_Text = [
        "Date",
        "Month",
        "Merchant",
        "Main category",
        "Second category",
        "Subcategory",
        "Location",
        "article",
        "category_main",
    ]
