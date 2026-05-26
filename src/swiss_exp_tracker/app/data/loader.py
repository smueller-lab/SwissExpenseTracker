from __future__ import annotations

import sqlite3

from pathlib import Path

import pandas as pd

from swiss_exp_tracker.app.config import DB_PATH
from swiss_exp_tracker.app.config import VIS
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
            self.pdf_Master = pd.read_sql("SELECT * FROM transactions_use", con)
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
            self.pdf_Balance = pd.read_sql("SELECT * FROM dash_balance", con)
            self.pdf_Balance = self.pdf_Balance.rename(
                columns={"date": "Date", "balance_chf": "Balance_CHF"}
            )
            self.pdf_Balance["Date"] = pd.to_datetime(self.pdf_Balance["Date"])

            # Groceries
            self.pdf_Grocery = pd.read_sql("SELECT * FROM dash_groceries", con)
            self.pdf_Grocery = self.pdf_Grocery.rename(columns={"merchant": "Merchant"})

            # Food
            self.pdf_Food = pd.read_sql("SELECT * FROM dash_food", con)

            # Category main (donut)
            self.pdf_CatMain = pd.read_sql("SELECT * FROM dash_cat_main", con)
            self.pdf_CatMain = self.pdf_CatMain.rename(columns={"amount": "amount_CHF"})

            # Stats KPIs — stored as single-row table, loaded as Series
            self.z_StatsTable = pd.read_sql("SELECT * FROM dash_stats", con).iloc[0]

            # Top category comparison
            self.pdf_TopCat = pd.read_sql("SELECT * FROM dash_top_category", con)
            self.pdf_TopCat["MonthLast"] = self.pdf_TopCat["MonthLast"].map(
                lambda s: pd.Period(str(s), "M")
            )

            # Top 20 expenses
            self.pdf_TopExpenses = pd.read_sql("SELECT * FROM dash_top_expenses", con)
            self.pdf_TopExpenses = self.pdf_TopExpenses.rename(
                columns={"date": "Date", "amount": "amount_CHF", "merchant": "Merchant"}
            )
            self.pdf_TopExpenses = self.apply_Variable_show(self.pdf_TopExpenses)

            # Net balance per month
            self.pdf_NetBalanceMonth = pd.read_sql(
                "SELECT Month, expense, income, NetBalance FROM dash_net_balance_month",
                con,
            )
            self.pdf_NetBalanceMonth = self.apply_Variable_show(
                self.pdf_NetBalanceMonth
            )
            self.max_data_year: int = int(
                self.pdf_NetBalanceMonth["Month"].str[:4].max()
            )

            # Vacation
            self.pdf_Vacation = pd.read_sql("SELECT * FROM dash_vacation", con)

            # Transport
            self.pdf_Transport = pd.read_sql("SELECT * FROM dash_transport", con)
            self.pdf_Transport = self.pdf_Transport.rename(
                columns={"amount": "amount_CHF"}
            )

            self.pdf_TransportHeatmap = pd.read_sql(
                "SELECT * FROM dash_transport_heatmap", con
            )
            self.pdf_TransportHeatmap = self.pdf_TransportHeatmap.rename(
                columns={"amount": "amount_CHF"}
            )

            # Sport
            self.pdf_Sport = pd.read_sql("SELECT * FROM dash_sport", con)

            # Car
            self.pdf_Car = pd.read_sql("SELECT * FROM dash_car", con)

            # Retail
            self.pdf_Retail = pd.read_sql("SELECT * FROM dash_retail", con)
            self.pdf_RetailDonut = pd.read_sql("SELECT * FROM dash_retail_donut", con)
            self.pdf_RetailTop = pd.read_sql("SELECT * FROM dash_retail_top", con)

            # Grocery detail (item-level receipt data)
            self.pdf_GroceryItems = pd.read_sql("SELECT * FROM groceries_use", con)
            self.pdf_GroceryItems["date"] = pd.to_datetime(
                self.pdf_GroceryItems["date"]
            )
            self.pdf_GroceryCat = pd.read_sql("SELECT * FROM dash_groceries_cat", con)
            self.pdf_GroceryHealth = pd.read_sql(
                "SELECT * FROM dash_groceries_health", con
            )
            self.pdf_GroceryTopArticles = pd.read_sql(
                "SELECT * FROM dash_groceries_top_articles", con
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

    def get_TopExpenses_Category_Month(self, Category: str, Month: str) -> pd.DataFrame:
        pdf = self.pdf_Master.copy()
        pdf["Month"] = pdf["date"].dt.to_period("M")
        pdf["Date"] = pdf["date"].dt.strftime("%d-%m-%Y")

        pdf = pdf[
            (pdf["category_main"] == Category)
            & (pdf["transaction_type"] == "EXPENSE")
            & (pdf["Month"] == Month)
        ].reset_index(drop=True)

        pdf = pdf.sort_values(by="amount_CHF", ascending=False).head(7)

        pdf = pdf.rename(columns={"city": "MerchantPlace"})

        s_col = ["Date", "amount_CHF", "Merchant", "category_second", "MerchantPlace"]
        return self.apply_Variable_show(pdf[s_col].copy())
