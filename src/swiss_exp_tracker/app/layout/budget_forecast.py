from __future__ import annotations

from typing import Any

from dash import html

from swiss_exp_tracker.app.components.cards import make_budget_forecast_card
from swiss_exp_tracker.app.components.cards import make_budget_input_card
from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.dash_components import make_page_title

cfg = config()

BUDGET_TABLE_COLUMNS: list[dict[str, Any]] = [
    {"id": "category", "name": "Category", "fmt": "text"},
    {"id": "budget_chf", "name": "Budget", "fmt": "chf"},
    {"id": "spend_chf", "name": "Spent", "fmt": "chf"},
    {"id": "forecast_chf", "name": "Forecast EOY", "fmt": "chf"},
    {"id": "pace_budget_chf", "name": "Budget used Now", "fmt": "chf"},
    {
        "id": "over_under_now_chf",
        "name": "Now Δ CHF",
        "fmt": "chf",
        "color": True,
        "invert": True,
    },
    {
        "id": "over_under_now_pct",
        "name": "Now Δ %",
        "fmt": "pct",
        "color": True,
        "invert": True,
    },
    {
        "id": "over_under_eoy_chf",
        "name": "EOY Δ CHF",
        "fmt": "chf",
        "color": True,
        "invert": True,
    },
    {
        "id": "over_under_eoy_pct",
        "name": "EOY Δ %",
        "fmt": "pct",
        "color": True,
        "invert": True,
    },
]


def layout(data: Any) -> Any:
    """Return the Budget / Forecasting page layout with input, forecast chart, and results table."""
    return html.Div(
        [
            make_page_title("\U0001f4b0 Budget / Forecasting"),
            html.Div(
                [
                    make_budget_input_card(
                        title="\U0001f4b0 Set Yearly Budgets",
                        categories=data.budget_categories_all,
                        saved_budgets=data.get_budgets(data.current_year),
                        year=data.current_year,
                        default_categories=cfg.budget_default_categories,
                        width=12,
                    ),
                    make_budget_forecast_card(
                        title="\U0001f4c8 Spend Progression & Year-End Forecast",
                        fig_id="budget-forecast-fig",
                        width=12,
                    ),
                    html.Div(id="budget-table-container", className="col-12"),
                ],
                className="grid",
            ),
        ]
    )
