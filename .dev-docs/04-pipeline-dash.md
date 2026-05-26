# 04 — Dashboard Pipeline

## Overview

The dashboard pipeline reads all transactions from `transactions_use`, applies a set of global exclusions, then builds a collection of pre-aggregated `dash_*` SQLite tables that the Dash app reads at startup. Each table is owned by one module under `src/swiss_exp_tracker/pipeline_dash/tables/`.

```
transactions_use  (SQLite)
        │
        ▼
run_dashboard_pipeline()          pipeline.py
        │
        ├─ _ensure_balance_chf()  ← migrate balance_chf column if absent
        │
        ├─ GLOBAL_EXCLUDE filter  ← drops rows before any table builder sees them
        │
        └─ table builders (in order):
              balance         → dash_balance
              groceries       → dash_groceries
              groceries_detail → dash_groceries_cat, dash_groceries_health, dash_groceries_top_articles
              food            → dash_food
              cat_main        → dash_cat_main
              stats           → dash_stats
              top_category    → dash_top_category
              top_expenses    → dash_top_expenses
              net_balance_month → dash_net_balance_month
              vacation        → dash_vacation
              transport       → dash_transport, dash_transport_heatmap
              sport           → dash_sport
              car             → dash_car
              retail          → dash_retail, dash_retail_donut, dash_retail_top
```

**Entry point:** `run_dashboard_pipeline()` in `pipeline.py`. Called after the ingestion pipeline completes. Defaults to the project database path from `pipeline_ingestion.config.INGESTION_DB_PATH`.

---

## Global exclusions (`config.GLOBAL_EXCLUDE`)

These rows are removed from the full DataFrame before it is passed to any table builder. Defined as `(category_main, category_second, merchant_substring | None)` tuples.

| category_main | category_second | merchant filter | Reason |
|---|---|---|---|
| Car | Purchase | — | One-off car purchase; distorts all transport and cost averages |
| Payment Services | Payment Fees | `"Viseca"` (case-insensitive) | Credit card bill settlement — internal account movement, not real spending |
| Payment Services | Money Transfer | — | Inter-account transfers — not real spending |

The merchant filter is a case-insensitive substring match. `None` means all merchants in that category/subcategory combination are excluded.

---

## Migration: `_ensure_balance_chf`

Runs before any table builder. Checks whether `balance_chf` exists in `transactions_rfn` and `transactions_use`. If absent (databases created before the column was added), it:

1. `ALTER TABLE transactions_rfn ADD COLUMN balance_chf REAL`
2. Backfills from `raw_json` — parses the `"Balance CHF"` field for `ZKB_DEBIT` rows.
3. `ALTER TABLE transactions_use ADD COLUMN balance_chf REAL`
4. Propagates from `transactions_rfn` via `reference` key.

This makes the pipeline safe to run on older databases without manual migration.

---

## Table builders

### `balance` → `dash_balance`

**Input filter:** `source_type == ZKB_DEBIT` and `balance_chf` is not null.

**Logic:**
- Groups by `date`, takes the last balance reading per day.
- Trims to the last 12 months from the most recent date.

**Used by:** Home page — Balance Progression line chart.

---

### `groceries` → `dash_groceries`

**Input filter:** `category_main == Groceries`, `transaction_type == EXPENSE`.

**Logic:**
- Normalises merchant names via substring matching (`config.GROCERY_MERCHANT_NORMALIZE`). First match wins. Example: any string containing `"migrolino"` → `"Migrolino"`.
- Keeps only the tracked merchants (`config.GROCERY_MERCHANTS_TRACKED`): Coop, Migros, Lidl, Aldi, Denner, Avec, Migrolino, K-Kiosk, Spar. All other grocery merchants are dropped.
- Computes monthly and yearly totals per merchant, plus the period total and percentage share per merchant within the period.

**Output columns:** `merchant`, `total_CHF`, `totalPeriod_CHF`, `pct`, `Freq` (`Monthly`/`Yearly`), `Period`.

**Used by:** Groceries page — absolute and percentage bar charts, per-visit box plot.

---

### `groceries_detail` → `dash_groceries_cat`, `dash_groceries_health`, `dash_groceries_top_articles`

**Input:** Reads directly from `groceries_use` (item-level receipt data). Does not use the `transactions_use` DataFrame passed by the pipeline.

**`dash_groceries_cat` logic:**
- Groups `groceries_use` by `(MonthYear, category_main)` and `(Year, category_main)`, sums `price_chf`.
- Adds `totalPeriod_CHF` (period total) and `pct` (share within period).
- Output columns: `category_main`, `total_CHF`, `totalPeriod_CHF`, `pct`, `Freq` (`Monthly`/`Yearly`), `Period`.

**`dash_groceries_health` logic:**
- Computes a Healthy Grocery Index score (0–100) per month using `HEALTH_WEIGHTS` — a per-`category_main` weight table defined in the module.
- Score = 50 + 50 × Σ(weight_i × share_i), clamped to [0, 100].
- Positive weights: Fresh Produce +1.0, Dairy & Eggs +0.6, Meat & Fish +0.4, Pasta & Grains +0.5, Baking +0.3.
- Negative weights: Snacks & Sweets −1.0, Ready Meals −0.8, Beverages −0.4, Frozen Foods −0.3, Canned & Preserved −0.1.
- Output columns: `Period` (YYYY-MM), `score`.

**`dash_groceries_top_articles` logic:**
- Filters `price_chf > 0` (removes bonus/return rows).
- Groups by `(article, category_main)`, aggregates count, total, and average `price_chf`.
- Sorted descending by purchase count.
- Output columns: `article`, `category_main`, `count`, `total_chf`, `avg_chf`.

**Used by:** M Cumulus Analytics page (`/m-cumulus`).

---

### `food` → `dash_food`

**Input filter:** `category_main` in `{Groceries, Restaurant}`, `transaction_type == EXPENSE`.

**Logic:**
- All Groceries rows get `category_second = "Groceries"` (they have no meaningful subcategory in a dining context).
- Restaurant rows keep their existing `category_second` (Dining, Fast Food, Cafe, Bar, Food Delivery).
- Computes monthly and yearly totals and percentage share per `category_second`.

**Output columns:** `category_second`, `total_CHF`, `totalPeriod_CHF`, `pct`, `Freq`, `Period`.

**Used by:** Dining page — grouped bar chart and box plot.

---

### `cat_main` → `dash_cat_main`

**Input filter:** `transaction_type == EXPENSE`.

**Logic:**
- Groups by `category_main` and `Year`, sums `amount`.
- Also creates an `"All"` year entry (all-time totals).
- Computes percentage share within each year.
- Categories with `perc < 1%` within a year are collapsed into a single `"Other"` bucket.

**Output columns:** `category_main`, `Year`, `amount`, `perc`.

**Used by:** Home page — Expense Distribution donut chart.

---

### `stats` → `dash_stats`

**Input filter:** All transaction types, but EXPENSE rows with `category_main` in `NET_BALANCE_EXPENSE_EXCLUDE_MAIN` (`Investing`, `Salary`) are excluded. These represent money movement rather than real spending.

**Logic:**
- Computes net = income − expense per month.
- Derives rolling 3-month and 12-month average net balance from the tail of the monthly series.
- Derives YTD net balance for the current year.
- Reads current balance from the latest ZKB Debit `balance_chf` value.

**Output columns (single row):** `Balance_current`, `Balance_net_3months`, `Balance_net_12months`, `Balance_net_currentYear`.

**Used by:** Home page — four KPI cards.

---

### `top_category` → `dash_top_category`

**Input filter:** `transaction_type == EXPENSE`, excludes `category_main` in `{Housing, Government}`. These are fixed costs (rent, taxes) that would always appear at the top and mask discretionary spending signals.

**Logic:**
- Determines the most recently *completed* month (if today is not month-end, uses the previous month-end).
- Computes per-`category_main` totals for: last completed month, the month before, and a 12-month rolling average.
- Calculates percentage differences (vs previous month, vs 12m average).

**Output columns:** `category_main`, `amount_MonthLast`, `amount_MonthPrev`, `amount_AVG_12m`, `diff_prev_pct`, `diff_12m_pct`, `MonthLast`.

**Used by:** Home page — Top Category KPI card.

---

### `top_expenses` → `dash_top_expenses`

**Input filter:** `transaction_type == EXPENSE`, excludes `category_main` in `TOP_EXPENSES_EXCLUDE_MAIN`: `Government, Finance, Friend, Housing, Financial Services, Healthcare, Investing, Insurance, Payment Services`. These are structural/fixed costs that are not informative as individual line items.

**Logic:**
- Rolling 12-month window from the latest transaction date.
- Takes the top 20 rows by `amount` descending.

**Output columns:** `date`, `amount`, `merchant`, `category_main`.

**Used by:** Home page — Top 20 Expenses table.

---

### `net_balance_month` → `dash_net_balance_month`

**Input filter:** All transaction types, but EXPENSE rows with `category_main` in `NET_BALANCE_EXPENSE_EXCLUDE_MAIN` (`Investing`, `Salary`) are excluded (same rationale as `stats`).

**Logic:**
- Pivots by `(Month, transaction_type)` → columns `expense`, `income`.
- Computes `NetBalance = income − expense` per month.
- Sorted descending (most recent first).

**Output columns:** `Month`, `expense`, `income`, `NetBalance`.

**Used by:** Home page — Net Balance per Month table.

---

### `vacation` → `dash_vacation`

**Input filter:** `category_main == Travel`, `transaction_type == EXPENSE`.

**Logic:**
- Groups by `Year` and `category_second` (Flight, Hotel, Hostel, Apartment, Car Rental, Ticket Booking).
- Sums `amount`.

**Output columns:** `Year`, `category_second`, `Total`.

**Used by:** Vacation page — stacked/grouped bar chart.

---

### `transport` → `dash_transport`, `dash_transport_heatmap`

**Input filter:** `category_main` in `{Car, Transport}`, `transaction_type == EXPENSE`.

**Exclusions (applied before both output tables):**
- `Car / Purchase` — removed by `GLOBAL_EXCLUDE` already at pipeline entry.
- Additionally: `TRANSPORT_EXCLUDE_SECOND = ["Purchase"]` guards against any residual Car Purchase rows.

**`dash_transport` logic:**
- Groups by `Year` and `category_transport` (= `category_second` if present, else `category_main`), sums `amount`.

**`dash_transport_heatmap` logic:**
- Additionally excludes `TRANSPORT_HEATMAP_EXCLUDE_SECOND = ["Service & Repair"]` — large irregular service costs that distort the regular commuting signal.
- Groups by `Year` and `Month_num`, sums `amount`, adds `Month_name`.

**Used by:** Transport page — yearly bar chart and monthly heatmap.

---

### `sport` → `dash_sport`

**Input filter:** `category_main == Sport`, `transaction_type == EXPENSE`.

**Exclusions:** `SPORT_EXCLUDE_SECOND = ["Sports Facility", "Unknown", "Sports Administration", "Sports services"]` — administrative and facility overhead, not personal sport activity costs.

**Logic:**
- Groups by `category_second` (Tennis, Golf, Padel, Bike, Fitness, Running, Swimming).
- Produces both yearly and monthly aggregates with `Freq` and `Period` columns.

**Output columns:** `category_sport`, `Total`, `Freq`, `Period`.

**Used by:** Sport page — switchable yearly/monthly bar chart.

---

### `car` → `dash_car`

**Input filter:** `category_main == Car`, `transaction_type == EXPENSE`.

**Exclusions:** `CAR_EXCLUDE_SECOND = ["Purchase", "Car Rental"]` — one-off capital expense and rentals which are not regular car ownership costs.

**Logic:**
- Groups by `category_second` (Fuel, Parking, Service & Repair, Wash, Tax).
- Produces yearly and monthly aggregates with `Freq` and `Period` columns.

**Output columns:** `category_car`, `Total`, `Freq`, `Period`.

**Used by:** Transport page — Car expenses switchable chart.

---

### `retail` → `dash_retail`, `dash_retail_donut`, `dash_retail_top`

**Input filter:** `category_main == Retail`, `transaction_type == EXPENSE`.

**`dash_retail` logic:**
- Groups by `category_second` (Clothing, Electronics, Home Goods, etc.).
- Yearly and monthly aggregates with `Freq` and `Period` columns.
- Output columns: `category_retail`, `Total`, `Freq`, `Period`.

**`dash_retail_donut` logic:**
- Groups by `Year` and `category_second`, sums `amount_CHF`.
- Also adds an `"All"` year entry (all-time totals).
- Output columns: `Year`, `category_retail`, `amount_CHF`.

**`dash_retail_top` logic:**
- Sorts all retail transactions by `amount` descending, takes top 10.
- Output columns: `Date`, `Merchant`, `Subcategory`, `amount_CHF`.

**Used by:** Retail page — bar chart, donut chart, top-10 table, and KPI cards.

---

## Config reference (`config.py`)

| Constant | Type | Purpose |
|---|---|---|
| `GLOBAL_EXCLUDE` | `list[tuple[str, str, str \| None]]` | Rows dropped before all builders run |
| `NET_BALANCE_EXPENSE_EXCLUDE_MAIN` | `list[str]` | Excluded from stats and net-balance month (`Investing`, `Salary`) |
| `TOP_EXPENSES_EXCLUDE_MAIN` | `list[str]` | Excluded from top-20 expenses table |
| `TRANSPORT_MAIN_CATEGORIES` | `list[str]` | `["Car", "Transport"]` |
| `TRANSPORT_EXCLUDE_SECOND` | `list[str]` | Excluded from transport bar chart |
| `TRANSPORT_HEATMAP_EXCLUDE_SECOND` | `list[str]` | Additionally excluded from heatmap |
| `SPORT_EXCLUDE_SECOND` | `list[str]` | Excluded from sport chart |
| `GROCERY_MERCHANT_NORMALIZE` | `list[tuple[str, str]]` | Substring → canonical name mapping |
| `GROCERY_MERCHANTS_TRACKED` | `list[str]` | Allowlist of grocery chains shown in charts |

---

## Adding a new table

1. Create `src/swiss_exp_tracker/pipeline_dash/tables/<name>.py` with a `build(df: pd.DataFrame, con: sqlite3.Connection) -> None` function.
2. Import the module in `pipeline.py` and add an entry to the `builders` list.
3. Add the new `dash_<name>` table to `app/data/loader.py` so the DataStore loads it.
4. Build the layout and callback in `app/layout/<name>.py` and `app/callbacks/<name>.py`.
