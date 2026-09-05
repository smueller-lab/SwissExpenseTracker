from __future__ import annotations

import calendar
import sqlite3

from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd

from swiss_exp_tracker.app.config import DB_PATH
from swiss_exp_tracker.app.config import VIS
from swiss_exp_tracker.app.data.budget_models import CategoryBudget
from swiss_exp_tracker.app.data.trip_models import Trip
from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_dash.config import BALANCE_SHEET_MAJOR_CATEGORIES
from swiss_exp_tracker.pipeline_dash.config import GROCERY_MERCHANT_NORMALIZE
from swiss_exp_tracker.pipeline_dash.config import GROCERY_MERCHANTS_TRACKED
from swiss_exp_tracker.pipeline_ingestion.db import migrate_trips_unique_name_year

_FREQ_RULE: dict[str, str] = {"D": "D", "W": "W", "M": "MS"}


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

            # Budget (create table defensively for tmp DBs in tests)
            transactions.create_dash_budget_table(con)
            self.pdf_Budget = pd.read_sql(transactions.get_dash_budget.sql, con)
            if self.pdf_Budget.empty:
                self.pdf_Budget = pd.DataFrame(
                    columns=["category", "year", "budget_chf", "updated_at"]
                )
            else:
                self.pdf_Budget["year"] = self.pdf_Budget["year"].astype(int)
                self.pdf_Budget["budget_chf"] = self.pdf_Budget["budget_chf"].astype(
                    float
                )

            self.current_year: int = (
                int(self.pdf_Master["date"].dt.year.max())
                if not self.pdf_Master.empty
                else 0
            )
            self.budget_categories: list[str] = sorted(
                self.pdf_Master.loc[
                    self.pdf_Master["transaction_type"] == "EXPENSE", "category_main"
                ]
                .unique()
                .tolist()
            )
            _expense = self.pdf_Master["transaction_type"] == "EXPENSE"
            _main = self.pdf_Master.loc[_expense, "category_main"].dropna()
            _second = self.pdf_Master.loc[_expense, "category_second"].dropna()
            self.budget_categories_all: list[str] = sorted(
                {str(c) for c in [*_main, *_second] if str(c).strip()}
            )

            # Trips (create tables defensively for tmp DBs in tests)
            self._defensive_create_trip_tables(con)
            self._reload_pdf_trips(con)

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

    def _net_balance_salary_months(self) -> pd.DataFrame:
        """Return pdf_NetBalanceMonth limited to the last 15 salary months, ascending.
        A salary month is any calendar month with at least one Salary transaction in pdf_Master.
        """
        salary_months: set[str] = {
            str(p)
            for p in self.pdf_Master.loc[
                self.pdf_Master["category_main"] == "Salary", "date"
            ].dt.to_period("M")
        }
        pdf = self.pdf_NetBalanceMonth.copy()
        pdf = pdf[pdf["Month"].isin(salary_months)]
        pdf = pdf.sort_values("Month", ascending=True)
        return pdf.tail(15).reset_index(drop=True)

    def get_IncomeExpenseMonthly(self) -> pd.DataFrame:
        """Return income/expense rows for the chart: salary months only, last 15, ascending."""
        return self._net_balance_salary_months()

    def get_NetBalanceMonthTable(self) -> pd.DataFrame:
        """Return the NetBalance table rows: same salary months as the chart, newest first.
        Drops months with no income yet so the table stays consistent with the chart.
        """
        return self._net_balance_salary_months().iloc[::-1].reset_index(drop=True)

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

    def get_budgets(self, year: int) -> list[CategoryBudget]:
        """Return stored budgets for year as CategoryBudget models."""
        rows = self.pdf_Budget[self.pdf_Budget["year"] == year]
        return [
            CategoryBudget.model_validate(
                {"category": r["category"], "budget_chf": r["budget_chf"]}
            )
            for _, r in rows.iterrows()
        ]

    def _reload_pdf_budget(self, con: sqlite3.Connection) -> None:
        """Reload self.pdf_Budget from the DB with normalized year/budget dtypes."""
        self.pdf_Budget = pd.read_sql(transactions.get_dash_budget.sql, con)
        if not self.pdf_Budget.empty:
            self.pdf_Budget["year"] = self.pdf_Budget["year"].astype(int)
            self.pdf_Budget["budget_chf"] = self.pdf_Budget["budget_chf"].astype(float)
        else:
            self.pdf_Budget = pd.DataFrame(
                columns=["category", "year", "budget_chf", "updated_at"]
            )

    def save_budgets(self, year: int, budgets: list[CategoryBudget]) -> None:
        """Upsert budgets for year to the DB, then reload self.pdf_Budget."""
        now = datetime.now().isoformat()
        with sqlite3.connect(str(self._db_path)) as con:
            transactions.create_dash_budget_table(con)
            for b in budgets:
                transactions.upsert_dash_budget(
                    con,
                    category=b.category,
                    budget_chf=b.budget_chf,
                    year=year,
                    updated_at=now,
                )
            self._reload_pdf_budget(con)

    def delete_budget(self, year: int, category: str) -> None:
        """Delete a category's budget for year so re-adding resets it to 0; reloads self.pdf_Budget."""
        with sqlite3.connect(str(self._db_path)) as con:
            transactions.create_dash_budget_table(con)
            transactions.delete_dash_budget(con, year=year, category=category)
            self._reload_pdf_budget(con)

    def _reload_pdf_trips(self, con: sqlite3.Connection) -> None:
        """Reload pdf_Trips, pdf_TripTransactionsDetail, and pdf_TripsByCategoryYear from DB and pdf_Master."""
        _TRIPS_COLS = [
            "id",
            "name",
            "year",
            "created_at",
            "updated_at",
            "total_chf",
            "n_transactions",
        ]
        _DETAIL_COLS = [
            "tt_id",
            "trip_id",
            "transaction_id",
            "assigned_at",
            "split",
            "date",
            "Merchant",
            "amount_CHF",
            "share_CHF",
            "category_main",
            "category_second",
            "transaction_type",
            "trip_name",
            "year",
        ]
        _BYCAT_COLS = ["year", "trip_id", "trip_name", "category_main", "total_chf"]

        pdf_trips_raw = pd.read_sql(transactions.get_trips.sql, con)

        if pdf_trips_raw.empty:
            self.pdf_Trips = pd.DataFrame(columns=_TRIPS_COLS)
            self.pdf_TripTransactionsDetail = pd.DataFrame(columns=_DETAIL_COLS)
            self.pdf_TripsByCategoryYear = pd.DataFrame(columns=_BYCAT_COLS)
            return

        pdf_tt = pd.read_sql(transactions.get_trip_transactions.sql, con)

        if not pdf_tt.empty:
            tt = pdf_tt.rename(columns={"id": "tt_id"})
            master_sub = self.pdf_Master[
                [
                    "id",
                    "date",
                    "Merchant",
                    "amount_CHF",
                    "category_main",
                    "category_second",
                    "transaction_type",
                ]
            ].copy()
            detail = tt.merge(
                master_sub, left_on="transaction_id", right_on="id", how="inner"
            ).drop(columns=["id"])
            trips_info = pdf_trips_raw[["id", "name", "year"]].rename(
                columns={"id": "trip_id", "name": "trip_name"}
            )
            detail = detail.merge(trips_info, on="trip_id", how="left")
            detail["share_CHF"] = detail["amount_CHF"] / detail["split"]
        else:
            detail = pd.DataFrame(columns=_DETAIL_COLS)

        self.pdf_TripTransactionsDetail = detail.reset_index(drop=True)

        if not detail.empty:
            detail["signed_amount_CHF"] = detail["share_CHF"].where(
                detail["transaction_type"] == "EXPENSE", -detail["share_CHF"]
            )
            agg = detail.groupby("trip_id", as_index=False).agg(
                total_chf=("signed_amount_CHF", "sum"),
                n_transactions=("transaction_id", "count"),
            )
        else:
            agg = pd.DataFrame(columns=["trip_id", "total_chf", "n_transactions"])

        pdf_trips_merged = pdf_trips_raw.merge(
            agg, left_on="id", right_on="trip_id", how="left"
        )
        pdf_trips_merged["total_chf"] = (
            pdf_trips_merged["total_chf"].infer_objects(copy=False).fillna(0.0)
        )
        pdf_trips_merged["n_transactions"] = (
            pdf_trips_merged["n_transactions"]
            .infer_objects(copy=False)
            .fillna(0)
            .astype(int)
        )
        self.pdf_Trips = pdf_trips_merged[_TRIPS_COLS].reset_index(drop=True)

        if not detail.empty:
            self.pdf_TripsByCategoryYear = (
                detail.groupby(["year", "trip_id", "trip_name", "category_main"])[
                    "signed_amount_CHF"
                ]
                .sum()
                .reset_index()
                .rename(columns={"signed_amount_CHF": "total_chf"})
            )
        else:
            self.pdf_TripsByCategoryYear = pd.DataFrame(columns=_BYCAT_COLS)

    def _defensive_create_trip_tables(self, con: sqlite3.Connection) -> None:
        """Create trips and trip_transactions tables/index/columns if they do not exist."""
        transactions.create_trips_table(con)
        migrate_trips_unique_name_year(con)
        transactions.create_trip_transactions_table(con)
        transactions.create_idx_trip_transactions_trip(con)
        tt_columns = {
            str(col[1]) for col in transactions.get_trip_transactions_column_names(con)
        }
        if "split" not in tt_columns:
            con.execute(transactions.alter_trip_transactions_add_split.sql)

    def create_trip(self, name: str, year: int) -> None:
        """Validate name/year, insert a new trip row, then reload trip DataFrames."""
        trip = Trip.model_validate({"name": name, "year": year})
        now = datetime.now().isoformat()
        with sqlite3.connect(str(self._db_path)) as con:
            self._defensive_create_trip_tables(con)
            transactions.insert_trip(
                con, name=trip.name, year=trip.year, created_at=now, updated_at=now
            )
            self._reload_pdf_trips(con)

    def rename_trip(self, trip_id: int, new_name: str) -> None:
        """Validate new_name, update the trip's name in the DB, then reload trip DataFrames."""
        validated = Trip.model_validate({"name": new_name, "year": 2000})
        now = datetime.now().isoformat()
        with sqlite3.connect(str(self._db_path)) as con:
            self._defensive_create_trip_tables(con)
            transactions.rename_trip(
                con, name=validated.name, updated_at=now, id=trip_id
            )
            self._reload_pdf_trips(con)

    def update_trip_year(self, trip_id: int, new_year: int) -> None:
        """Validate new_year, update the trip's year in the DB, then reload trip DataFrames."""
        validated = Trip.model_validate({"name": "placeholder", "year": new_year})
        now = datetime.now().isoformat()
        with sqlite3.connect(str(self._db_path)) as con:
            self._defensive_create_trip_tables(con)
            transactions.update_trip_year(
                con, year=validated.year, updated_at=now, id=trip_id
            )
            self._reload_pdf_trips(con)

    def delete_trip(self, trip_id: int) -> None:
        """Delete a trip's junction rows then the trip itself, then reload trip DataFrames."""
        with sqlite3.connect(str(self._db_path)) as con:
            self._defensive_create_trip_tables(con)
            transactions.delete_trip_transactions_by_trip(con, trip_id=trip_id)
            transactions.delete_trip(con, id=trip_id)
            self._reload_pdf_trips(con)

    def assign_transactions_to_trip(
        self, trip_id: int, transaction_ids: list[int]
    ) -> None:
        """Upsert one or more transactions into a trip (moves already-assigned ones), then reload trip DataFrames."""
        now = datetime.now().isoformat()
        rows = [
            {"trip_id": trip_id, "transaction_id": tid, "assigned_at": now}
            for tid in transaction_ids
        ]
        with sqlite3.connect(str(self._db_path)) as con:
            self._defensive_create_trip_tables(con)
            transactions.assign_transactions_to_trip(con, rows)
            self._reload_pdf_trips(con)

    def unassign_transactions(self, transaction_ids: list[int]) -> None:
        """Remove trip assignments for the given transaction IDs, then reload trip DataFrames."""
        with sqlite3.connect(str(self._db_path)) as con:
            self._defensive_create_trip_tables(con)
            for tid in transaction_ids:
                transactions.unassign_transaction_from_trip(con, transaction_id=tid)
            self._reload_pdf_trips(con)

    def update_transaction_split(self, transaction_id: int, split: int) -> None:
        """Set how many people share an assigned transaction's cost, then reload trip DataFrames."""
        with sqlite3.connect(str(self._db_path)) as con:
            self._defensive_create_trip_tables(con)
            transactions.update_trip_transaction_split(
                con, split=max(1, split), transaction_id=transaction_id
            )
            self._reload_pdf_trips(con)

    def get_unassigned_transactions(self) -> pd.DataFrame:
        """Return pdf_Master rows with no trip assignment, sorted by date descending."""
        if self.pdf_TripTransactionsDetail.empty:
            return self.pdf_Master.sort_values("date", ascending=False).reset_index(
                drop=True
            )
        assigned_ids = set(self.pdf_TripTransactionsDetail["transaction_id"].tolist())
        mask = ~self.pdf_Master["id"].isin(assigned_ids)
        return (
            self.pdf_Master[mask]
            .sort_values("date", ascending=False)
            .reset_index(drop=True)
        )

    def get_category_year_spend(self, year: int) -> pd.DataFrame:
        """Return per-category EXPENSE totals for year (main and second levels); columns [category, spend_chf]."""
        mask = (self.pdf_Master["transaction_type"] == "EXPENSE") & (
            self.pdf_Master["date"].dt.year == year
        )
        base = self.pdf_Master[mask]
        parts: list[pd.DataFrame] = []
        for level in ("category_main", "category_second"):
            grouped = (
                base.dropna(subset=[level])
                .groupby(level)["amount_CHF"]
                .sum()
                .reset_index()
                .rename(columns={level: "category", "amount_CHF": "spend_chf"})
            )
            parts.append(grouped)
        combined = pd.concat(parts, ignore_index=True)
        combined = combined[combined["category"].astype(str).str.strip() != ""]
        return combined.drop_duplicates(subset="category", keep="first").reset_index(
            drop=True
        )

    def get_category_cumulative(
        self,
        year: int,
        categories: list[str],
        freq: Literal["D", "W", "M"],
    ) -> pd.DataFrame:
        """Return long-format cumulative EXPENSE spend per category for year at freq.
        Columns: category, period_end (datetime64), cumulative_chf (non-decreasing within category).
        """
        rule = _FREQ_RULE[freq]
        mask = (
            (self.pdf_Master["transaction_type"] == "EXPENSE")
            & (self.pdf_Master["date"].dt.year == year)
            & (
                self.pdf_Master["category_main"].isin(categories)
                | self.pdf_Master["category_second"].isin(categories)
            )
        )
        pdf = self.pdf_Master[mask][
            ["date", "category_main", "category_second", "amount_CHF"]
        ].copy()

        results: list[pd.DataFrame] = []
        for cat in categories:
            cat_mask = (pdf["category_main"] == cat) | (pdf["category_second"] == cat)
            cat_series = pdf[cat_mask].set_index("date")["amount_CHF"]
            if cat_series.empty:
                continue
            cumulative = cat_series.resample(rule).sum().cumsum()
            results.append(
                pd.DataFrame(
                    {
                        "category": cat,
                        "period_end": cumulative.index.to_numpy(),
                        "cumulative_chf": cumulative.to_numpy(),
                    }
                )
            )

        if not results:
            return pd.DataFrame(columns=["category", "period_end", "cumulative_chf"])
        return pd.concat(results, ignore_index=True)

    def get_category_period_history(
        self,
        categories: list[str],
        exclude_year: int,
        freq: Literal["D", "W", "M"],
    ) -> pd.DataFrame:
        """Return per-category per-period EXPENSE spend for all years before exclude_year.
        Columns: category, year (int), year_fraction (float in (0,1]), spend_chf.
        """
        rule = _FREQ_RULE[freq]
        mask = (
            (self.pdf_Master["transaction_type"] == "EXPENSE")
            & (self.pdf_Master["date"].dt.year < exclude_year)
            & (
                self.pdf_Master["category_main"].isin(categories)
                | self.pdf_Master["category_second"].isin(categories)
            )
        )
        pdf = self.pdf_Master[mask][
            ["date", "category_main", "category_second", "amount_CHF"]
        ].copy()

        records: list[dict[str, object]] = []
        for cat in categories:
            cat_pdf = pdf[
                (pdf["category_main"] == cat) | (pdf["category_second"] == cat)
            ]
            if cat_pdf.empty:
                continue
            year_vals = sorted(cat_pdf["date"].dt.year.unique())
            for year_raw in year_vals:
                year_int = int(year_raw)
                year_series = cat_pdf[cat_pdf["date"].dt.year == year_int].set_index(
                    "date"
                )["amount_CHF"]
                resampled = year_series.resample(rule).sum()
                # Keep only buckets whose label date falls within the year
                resampled = resampled[
                    pd.DatetimeIndex(resampled.index).year == year_int
                ]
                n_days = 366 if calendar.isleap(year_int) else 365
                for period_dt, spend in resampled.items():
                    period_ts = pd.Timestamp(period_dt)
                    records.append(
                        {
                            "category": cat,
                            "year": year_int,
                            "year_fraction": period_ts.dayofyear / n_days,
                            "spend_chf": float(spend),
                        }
                    )

        if not records:
            return pd.DataFrame(
                columns=["category", "year", "year_fraction", "spend_chf"]
            )
        return pd.DataFrame(records)

    def get_category_monthly_totals(
        self,
        categories: list[str],
        as_of: pd.Timestamp,
        n_years: int,
    ) -> dict[str, list[float]]:
        """Return per-category completed-month EXPENSE totals for the n_years years before
        as_of's year, plus as_of's year through its last fully completed month. Prior years are
        zero-filled across all 12 months; the current year stops at the last completed month so
        medians aren't dragged toward zero by months that haven't happened yet. Used to gauge a
        category's lumpiness and its robust monthly spend rate.
        """
        current_year = as_of.year
        start_year = current_year - n_years
        mask = (
            (self.pdf_Master["transaction_type"] == "EXPENSE")
            & (self.pdf_Master["date"].dt.year >= start_year)
            & (self.pdf_Master["date"].dt.year <= current_year)
            & (
                self.pdf_Master["category_main"].isin(categories)
                | self.pdf_Master["category_second"].isin(categories)
            )
        )
        pdf = self.pdf_Master[mask][
            ["date", "category_main", "category_second", "amount_CHF"]
        ].copy()

        last_complete_month = as_of.month - 1  # current month may still be in progress

        totals: dict[str, list[float]] = {}
        for cat in categories:
            cat_mask = (pdf["category_main"] == cat) | (pdf["category_second"] == cat)
            cat_series = pdf[cat_mask].set_index("date")["amount_CHF"]
            if cat_series.empty:
                totals[cat] = []
                continue
            months: list[float] = []
            for yr in range(start_year, current_year + 1):
                n_months = 12 if yr < current_year else last_complete_month
                if n_months <= 0:
                    continue
                full = pd.date_range(f"{yr}-01-01", periods=n_months, freq="MS")
                monthly = (
                    cat_series[pd.DatetimeIndex(cat_series.index).year == yr]
                    .resample("MS")
                    .sum()
                    .reindex(full, fill_value=0.0)
                )
                months.extend(float(v) for v in monthly.to_numpy())
            totals[cat] = months
        return totals
