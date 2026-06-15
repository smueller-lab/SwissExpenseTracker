from __future__ import annotations

from typing import Any

from dash import html

from swiss_exp_tracker.app.components.cards import make_balance_sheet_card
from swiss_exp_tracker.app.components.cards import make_category_spend_card
from swiss_exp_tracker.app.components.cards import make_number_card
from swiss_exp_tracker.app.dash_components import make_page_title


def layout(data: Any) -> Any:
    """Return the Balance Sheet page layout with KPI cards and yearly summary tables."""
    return html.Div(
        [
            make_page_title("📊 Balance Sheet"),
            html.Div(
                [
                    make_number_card(
                        "Lifetime Net Gain",
                        data.v_BS_LifetimeNetGain,
                        width=3,
                        unit="CHF",
                        fmt=",.0f",
                    ),
                    make_number_card(
                        "Total Invested (All-Time)",
                        data.v_BS_TotalInvestedAllTime,
                        width=3,
                        unit="CHF",
                        fmt=",.0f",
                    ),
                    make_number_card(
                        "Avg Gain Rate",
                        data.v_BS_SavingsRateAvg,
                        width=3,
                        unit="%",
                        fmt=".1f",
                    ),
                    make_number_card(
                        "Avg Annual Net Gain",
                        data.v_BS_AvgAnnualNetGain,
                        width=3,
                        unit="CHF",
                        fmt=",.0f",
                    ),
                    make_balance_sheet_card(
                        "Yearly Balance Sheet",
                        data.pdf_BalanceSheet,
                        width=12,
                    ),
                    make_category_spend_card(
                        "Major Category Spend by Year",
                        data.pdf_CategorySpendPivot.reset_index(),
                        data.pdf_CategorySpendYoY.reset_index(),
                        width=12,
                    ),
                ],
                className="grid",
            ),
        ]
    )
