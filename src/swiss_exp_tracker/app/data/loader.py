from __future__ import annotations

import calendar
import sqlite3

from pathlib import Path

import pandas as pd

from swiss_exp_tracker.app.config import DB_PATH
from swiss_exp_tracker.app.config import VIS
from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_dash.config import BALANCE_SHEET_MAJOR_CATEGORIES
from swiss_exp_tracker.pipeline_dash.config import GROCERY_MERCHANT_NORMALIZE
from swiss_exp_tracker.pipeline_dash.config import GROCERY_MERCHANTS_TRACKED


def _normalize_merchant(merchant: str) -> str:
    m = merchant.lower()
    for pattern, canonical in GROCERY_MERCHANT_NORMALIZE:
        if pattern in m:
            return canonical
    return merchant


class DataLoader:
    def __init__(self, db_path: Path | str | None = None) -> None:
        self.vis = VIS()
        self._db_path: Path | str = db_path or DB_PATH
        self._load_all_data()

    def get_scol_DashTable(self, pdf: pd.DataFrame) -> list[dict[str, object]]:
        return [
            {
                "name": col,
                "id": col,
                **self.vis.vk_format_col.get(col, {"type": "text"}),
            }
            for col in pdf.columns
        ]

    def apply_Variable_show(self, pdf: pd.DataFrame) -> pd.DataFrame:
        return pdf.rename(columns=self.vis.vk_Variable_show)

    def _load_all_data(self) -> None:
        with sqlite3.connect(str(self._db_path)) as con:
            # Raw transactions for ad-hoc queries (get_TopExpenses_Category_Month)
            self.pdf_Master = pd.read_sql(transactions.get_transactions_use.sql, con)
            self.pdf_Master["date"] = pd.to_datetime(self.pdf_Master["date"])
            self.pdf_Master = self.pdf_Master.rename(
                columns={"merchant": "Merchant", "amount": "amount_CHF"}
            )

            # Per-visit grocery amounts with normalized merchant names (for box plot)
            pdf_gv = self.pdf_Master[
                (self.pdf_Master["category_main"] == "Groceries")
                & (self.pdf_Master["transaction_type"] == "EXPENSE")
            ].copy()
            pdf_gv["Merchant"] = pdf_gv["Merchant"].apply(_normalize_merchant)
            self.pdf_GroceryVisits = pdf_gv[
                pdf_gv["Merchant"].isin(GROCERY_MERCHANTS_TRACKED)
            ].reset_index(drop=True)

            # Balance
            self.pdf_Balance = pd.read_sql(transactions.get_dash_balance.sql, con)
            self.pdf_Balance = self.pdf_Balance.rename(
                columns={"date": "Date", "balance_chf": "Balance_CHF"}
            )
            self.pdf_Balance["Date"] = pd.to_datetime(self.pdf_Balance["Date"])

            # Groceries
            self.pdf_Grocery = pd.read_sql(transactions.get_dash_groceries.sql, con)
            self.pdf_Grocery = self.pdf_Grocery.rename(columns={"merchant": "Merchant"})

            # Food
            self.pdf_Food = pd.read_sql(transactions.get_dash_food.sql, con)

            # Category main (donut)
            self.pdf_CatMain = pd.read_sql(transactions.get_dash_cat_main.sql, con)
            self.pdf_CatMain = self.pdf_CatMain.rename(columns={"amount": "amount_CHF"})

            # Stats KPIs — stored as single-row table, loaded as Series
            self.z_StatsTable = pd.read_sql(transactions.get_dash_stats.sql, con).iloc[
                0
            ]

            # Top category comparison
            self.pdf_TopCat = pd.read_sql(transactions.get_dash_top_category.sql, con)
            self.pdf_TopCat["MonthLast"] = self.pdf_TopCat["MonthLast"].map(
                lambda s: pd.Period(str(s), "M")
            )

            # Top 20 expenses
            self.pdf_TopExpenses = pd.read_sql(
                transactions.get_dash_top_expenses.sql, con
            )
            self.pdf_TopExpenses = self.pdf_TopExpenses.rename(
                columns={"date": "Date", "amount": "amount_CHF", "merchant": "Merchant"}
            )
            self.pdf_TopExpenses = self.apply_Variable_show(self.pdf_TopExpenses)

            # Net balance per month
            self.pdf_NetBalanceMonth = pd.read_sql(
                transactions.get_dash_net_balance_month.sql,
                con,
            )
            self.pdf_NetBalanceMonth = self.apply_Variable_show(
                self.pdf_NetBalanceMonth
            )
            self.max_data_year: int = int(
                self.pdf_NetBalanceMonth["Month"].str[:4].max()
            )

            # Vacation
            self.pdf_Vacation = pd.read_sql(transactions.get_dash_vacation.sql, con)
            self.pdf_VacationTransactions = pd.read_sql(
                transactions.get_dash_vacation_transactions.sql, con
            )

            # Transport
            self.pdf_Transport = pd.read_sql(transactions.get_dash_transport.sql, con)
            self.pdf_Transport = self.pdf_Transport.rename(
                columns={"amount": "amount_CHF"}
            )

            self.pdf_TransportHeatmap = pd.read_sql(
                transactions.get_dash_transport_heatmap.sql, con
            )
            self.pdf_TransportHeatmap = self.pdf_TransportHeatmap.rename(
                columns={"amount": "amount_CHF"}
            )

            # Sport
            self.pdf_Sport = pd.read_sql(transactions.get_dash_sport.sql, con)

            self.pdf_SportActivities: pd.DataFrame = pd.read_sql(
                transactions.get_dash_sport_activities.sql, con
            )

            pdf_yearly = self.pdf_Sport[self.pdf_Sport["Freq"] == "Yearly"]
            if pdf_yearly.empty:
                self.v_SportAvgPerYear: float = 0.0
            else:
                self.v_SportAvgPerYear = float(
                    pdf_yearly["Total"].sum() / pdf_yearly["Period"].nunique()
                )

            pdf_monthly = self.pdf_Sport[self.pdf_Sport["Freq"] == "Monthly"].copy()
            if pdf_monthly.empty:
                self.v_SportCurrentYtd: float = 0.0
                self.v_SportPctVsLastYtd: float | None = None
                self.s_SportYtdLabel: str = "Current Year"
            else:
                _years = pdf_monthly["Period"].str[:4].astype(int)
                _months = pdf_monthly["Period"].str[5:7].astype(int)
                pdf_monthly = pdf_monthly.assign(_year=_years, _month=_months)

                current_year = int(pdf_monthly["_year"].max())
                max_month = int(
                    pdf_monthly.loc[
                        pdf_monthly["_year"] == current_year, "_month"
                    ].max()
                )

                mask_current = (pdf_monthly["_year"] == current_year) & (
                    pdf_monthly["_month"] <= max_month
                )
                current_ytd = float(pdf_monthly.loc[mask_current, "Total"].sum())
                self.v_SportCurrentYtd = current_ytd

                last_year = current_year - 1
                mask_last = (pdf_monthly["_year"] == last_year) & (
                    pdf_monthly["_month"] <= max_month
                )
                last_year_ytd = float(pdf_monthly.loc[mask_last, "Total"].sum())
                if last_year_ytd == 0:
                    self.v_SportPctVsLastYtd = None
                else:
                    self.v_SportPctVsLastYtd = (
                        (current_ytd - last_year_ytd) / last_year_ytd * 100
                    )

                month_abbr = calendar.month_abbr[max_month]
                self.s_SportYtdLabel = f"YTD {month_abbr} {current_year}"

            # Car
            self.pdf_Car = pd.read_sql(transactions.get_dash_car.sql, con)

            # Retail
            self.pdf_Retail = pd.read_sql(transactions.get_dash_retail.sql, con)
            self.pdf_RetailDonut = pd.read_sql(
                transactions.get_dash_retail_donut.sql, con
            )
            self.pdf_RetailTop = pd.read_sql(transactions.get_dash_retail_top.sql, con)

            # Grocery detail (item-level receipt data)
            self.pdf_GroceryItems = pd.read_sql(transactions.get_groceries_use.sql, con)
            self.pdf_GroceryItems["date"] = pd.to_datetime(
                self.pdf_GroceryItems["date"]
            )
            self.pdf_GroceryCat = pd.read_sql(
                transactions.get_dash_groceries_cat.sql, con
            )
            self.pdf_GroceryHealth = pd.read_sql(
                transactions.get_dash_groceries_health.sql, con
            )
            self.pdf_GroceryTopArticles = pd.read_sql(
                transactions.get_dash_groceries_top_articles.sql, con
            )

            if not self.pdf_GroceryItems.empty:
                _curr = self.pdf_GroceryItems["date"].dt.to_period("M").max()
                _curr_items = self.pdf_GroceryItems[
                    self.pdf_GroceryItems["date"].dt.to_period("M") == _curr
                ]
                self.n_GroceryMonthlySpend: float = float(
                    _curr_items["price_chf"].sum()
                )
                self.n_GroceryVisitsMonth: int = int(
                    _curr_items.groupby(["date", "location"]).ngroups
                )
                self.n_GroceryAvgPerVisit: float = (
                    self.n_GroceryMonthlySpend / self.n_GroceryVisitsMonth
                    if self.n_GroceryVisitsMonth > 0
                    else 0.0
                )
                self.s_GroceryCurrentMonth: str = _curr.strftime("%b %Y")
            else:
                self.n_GroceryMonthlySpend = 0.0
                self.n_GroceryVisitsMonth = 0
                self.n_GroceryAvgPerVisit = 0.0
                self.s_GroceryCurrentMonth = ""

            self.n_HealthScore_latest: float = (
                float(self.pdf_GroceryHealth.iloc[-1]["score"])
                if not self.pdf_GroceryHealth.empty
                else 50.0
            )

            # Balance sheet — yearly aggregates and major-category spend
            self.pdf_BalanceSheet = pd.read_sql(
                transactions.get_dash_balance_sheet.sql, con
            )
            self.pdf_BalanceSheetCategories = pd.read_sql(
                transactions.get_dash_balance_sheet_categories.sql, con
            )

        # Category labels in the canonical order defined in pipeline config
        _cat_labels = [label for label, _, _ in BALANCE_SHEET_MAJOR_CATEGORIES]

        if self.pdf_BalanceSheetCategories.empty:
            self.pdf_CategorySpendPivot: pd.DataFrame = pd.DataFrame(
                columns=_cat_labels
            )
            self.pdf_CategorySpendYoY: pd.DataFrame = pd.DataFrame(columns=_cat_labels)
        else:
            _pivot = self.pdf_BalanceSheetCategories.pivot_table(
                index="year",
                columns="category",
                values="amount",
                aggfunc="sum",
                fill_value=0.0,
            )
            # Reindex to canonical category order; fill any missing category with 0
            _pivot = _pivot.reindex(columns=_cat_labels, fill_value=0.0)
            # Newest year at the top
            _pivot = _pivot.sort_index(ascending=False)
            self.pdf_CategorySpendPivot = _pivot

            # YoY % change: (current - prior) / |prior| * 100; oldest row -> NaN
            _pivot_asc = _pivot.sort_index(ascending=True)
            _yoy_asc = (
                (_pivot_asc - _pivot_asc.shift(1)) / _pivot_asc.shift(1).abs() * 100
            )
            self.pdf_CategorySpendYoY = _yoy_asc.sort_index(ascending=False)

        if self.pdf_BalanceSheet.empty:
            self.v_BS_LatestYear: int = 0
            self.v_BS_LifetimeNetGain: float = 0.0
            self.v_BS_TotalInvestedAllTime: float = 0.0
            self.v_BS_SavingsRateAvg: float = 0.0
            self.v_BS_AvgAnnualNetGain: float = 0.0
        else:
            _latest = self.pdf_BalanceSheet.iloc[0]  # newest row (sorted DESC)
            self.v_BS_LatestYear = int(_latest["year"])
            # cumulative_saved in the newest row == sum of all yearly Net Gain
            self.v_BS_LifetimeNetGain = float(_latest["cumulative_saved"])
            self.v_BS_TotalInvestedAllTime = float(
                self.pdf_BalanceSheet["invested"].sum()
            )
            self.v_BS_SavingsRateAvg = float(
                self.pdf_BalanceSheet["savings_rate_pct"].mean()
            )
            self.v_BS_AvgAnnualNetGain = float(
                self.pdf_BalanceSheet["total_plus"].mean()
            )

    def get_TopExpenses_Category_Month(
        self, Category: str, Month: pd.Timestamp
    ) -> pd.DataFrame:
        """Return top 7 expenses for Category in the month of Month."""
        pdf = self.pdf_Master.copy()
        pdf["Month"] = pdf["date"].dt.to_period("M")
        pdf["Date"] = pdf["date"].dt.strftime("%d-%m-%Y")
        month_period = pd.Period(Month, "M")

        pdf = pdf[
            (pdf["category_main"] == Category)
            & (pdf["transaction_type"] == "EXPENSE")
            & (pdf["Month"] == month_period)
        ].reset_index(drop=True)

        pdf = pdf.sort_values(by="amount_CHF", ascending=False).head(7)

        s_col = ["Date", "amount_CHF", "Merchant", "category_second"]
        return self.apply_Variable_show(pdf[s_col].copy())
