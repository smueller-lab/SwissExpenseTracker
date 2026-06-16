from __future__ import annotations

from typing import Any

import pandas as pd

from dash import html

from swiss_exp_tracker.app.components.cards import make_CategoryDonut_card
from swiss_exp_tracker.app.components.cards import make_figure_card
from swiss_exp_tracker.app.components.cards import make_number_card
from swiss_exp_tracker.app.components.cards import make_table_card
from swiss_exp_tracker.app.components.cards import make_TopCategory_card
from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()


def layout(data: Any, pos: Any) -> Any:
    if data.pdf_TopCat.empty:
        top_category: str = "—"
        month_last: pd.Timestamp = pd.Timestamp.today()
        amount_month_last: float = 0.0
        amount_month_prev: float = 0.0
        amount_avg_12m: float = 0.0
        diff_prev_pct: float = 0.0
        diff_12m_pct: float = 0.0
    else:
        _row = data.pdf_TopCat.sort_values(by="amount_MonthLast", ascending=False).iloc[
            0
        ]
        top_category = str(_row["category_main"])
        month_last = _row["MonthLast"].to_timestamp()
        amount_month_last = float(_row["amount_MonthLast"])
        amount_month_prev = float(_row["amount_MonthPrev"])
        amount_avg_12m = float(_row["amount_AVG_12m"])
        diff_prev_pct = float(_row["diff_prev_pct"])
        diff_12m_pct = float(_row["diff_12m_pct"])

    month_last_pretty: str = month_last.strftime("%Y-%B")
    pdf_TopExpenses_Category_Month = data.get_TopExpenses_Category_Month(
        Category=top_category, Month=month_last
    )

    return html.Div(
        [
            make_number_card("Current Balance", data.z_StatsTable["Balance_current"]),
            make_number_card("Portfolio Value", pos.total_value),
            make_number_card(
                f"Total Savings ({data.max_data_year})",
                data.z_StatsTable["Balance_net_currentYear"],
            ),
            make_number_card(
                "Avg Expenses (12m)",
                data.z_StatsTable["Expense_avg_12months"],
            ),
            ##
            make_figure_card(
                "Income vs Expenses",
                F.fig_IncomeExpenseMonthly(data.get_IncomeExpenseMonthly()),
                width=8,
            ),
            make_table_card(
                title="NetBalance per Month",
                s_col=data.get_scol_DashTable(data.pdf_NetBalanceMonth),
                data=data.pdf_NetBalanceMonth.to_dict("records"),
                width=4,
            ),
            ##
            make_TopCategory_card(
                title="Top Category",
                MonthLast=month_last_pretty,
                Category=top_category,
                amount_MonthLast=amount_month_last,
                amount_MonthPrev=amount_month_prev,
                amount_12m_avg=amount_avg_12m,
                diff_prev_pct=diff_prev_pct,
                diff_12m_pct=diff_12m_pct,
                width=4,
            ),
            make_table_card(
                title=f"Top Expenses - {top_category} - {month_last_pretty}",
                s_col=data.get_scol_DashTable(pdf_TopExpenses_Category_Month),
                data=pdf_TopExpenses_Category_Month.to_dict("records"),
                width=8,
            ),
            ##
            make_CategoryDonut_card("Expense distribution", data.pdf_CatMain, width=5),
            make_table_card(
                title="Top 20 Expenses",
                s_col=data.get_scol_DashTable(data.pdf_TopExpenses),
                data=data.pdf_TopExpenses.to_dict("records"),
                width=7,
            ),
        ],
        className="grid",
    )
