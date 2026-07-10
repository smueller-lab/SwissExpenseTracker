# Dashboard Structure

The dashboard is a Plotly Dash application (`app/`). Each page has a `layout/` module that builds the static HTML structure and a `callbacks/` module that wires up the interactive elements. Data is served from pre-aggregated SQLite tables produced by the `pipeline_dash` pipeline in `src/`.

---

## Page overview

| Page | Route | Layout file | Callbacks file |
|---|---|---|---|
| Home | `/` | `layout/home.py` | `callbacks/home.py` |
| Balance Sheet | `/balance-sheet` | `layout/balance_sheet.py` | — |
| Budget / Forecasting | `/budget-forecast` | `layout/budget_forecast.py` | `callbacks/budget_forecast.py` |
| Transport | `/transport` | `layout/transport.py` | `callbacks/transport.py` |
| Dining | `/food` | `layout/food.py` | `callbacks/food.py` |
| Groceries | `/groceries` | `layout/groceries.py` | `callbacks/groceries.py` |
| M Cumulus Analytics | `/m-cumulus` | `layout/groceries_detail.py` | `callbacks/groceries_detail.py` |
| Sport | `/sport` | `layout/sport.py` | `callbacks/sport.py` |
| Vacation | `/vacation` | `layout/vacation.py` | — |
| Retail | `/retail` | `layout/retail.py` | `callbacks/retail.py` |
| Smart Table | `/smarttable` | `layout/smarttable.py` | `callbacks/smarttable.py` |
| Investing | `/investing` | `layout/investing.py` | `callbacks/investing.py` |

---

## Home

**KPI cards (top row)**

| Card | Value | Source |
|---|---|---|
| Current Balance | Latest ZKB Debit account balance (CHF) | `dash_stats.Balance_current` |
| Avg net Balance 3 mo | Rolling 3-month average of income − expense | `dash_stats.Balance_net_3months` |
| Avg net Balance 12 mo | Rolling 12-month average of income − expense | `dash_stats.Balance_net_12months` |
| Net Balance (current year) | YTD income − expense | `dash_stats.Balance_net_currentYear` |

**Stats exclusions:** Expense categories `Investing` and `Salary` are excluded from all net-balance and stats calculations — these represent money movement (transfers to broker, salary receipt reversal), not real spending. Defined in `config.NET_BALANCE_EXPENSE_EXCLUDE_MAIN`.

**Balance Progression chart** (`fig_BalancePerDay`)
- Line chart showing daily account balance over time.
- Source: `pdf_Balance` — raw balance data from `dash_balance`.

**Net Balance per Month table**
- Monthly income / expense / net columns, sorted descending by month.
- Source: `dash_net_balance_month`.
- Same `Investing` + `Salary` exclusion applies.

**Top Category card**
- Highlights the single highest-spend expense category in the most recently completed month.
- Shows amount last month, amount previous month, 12-month average, and percentage differences.
- Excluded categories: `Housing` and `Government` (rent and taxes are fixed costs that distort the "interesting" top category signal).
- Source: `dash_top_category`.

**Top Expenses table** (for top category)
- Lists the individual transactions in the top category for that month.
- Source: `pdf_Master` filtered to the top category + month.

**Expense Distribution donut chart** (`fig_CategoryDonut`)
- Proportional breakdown of all-time EXPENSE amounts by `category_main`.
- Source: `dash_cat_main`.

**Top 20 Expenses table**
- The 20 largest single EXPENSE transactions from the last 12 months.
- Excluded `category_main` values (non-discretionary noise): `Government`, `Finance`, `Friend`, `Housing`, `Financial Services`, `Healthcare`, `Investing`, `Insurance`, `Payment Services`.
- Defined in `config.TOP_EXPENSES_EXCLUDE_MAIN`.
- Source: `dash_top_expenses`.

---

## Balance Sheet

Pipeline table: `pipeline_dash/tables/balance_sheet.py` → `dash_balance_sheet`, `dash_balance_sheet_categories`

**KPI cards (top row)**

| Card | Value | Source |
|---|---|---|
| Lifetime Net Gain | Cumulative `total_plus` (saved + invested) across all years | `data.v_BS_LifetimeNetGain` |
| Total Invested (All-Time) | Sum of yearly `invested` (EXPENSE, `category_main == Investing`) | `data.v_BS_TotalInvestedAllTime` |
| Avg Gain Rate | Mean of yearly `savings_rate_pct` (`total_plus / income × 100`) | `data.v_BS_SavingsRateAvg` |
| Avg Annual Net Gain | Mean of yearly `total_plus` | `data.v_BS_AvgAnnualNetGain` |

Same `Investing` + `Salary`-style exclusion as Home: `expense` excludes `NET_BALANCE_EXPENSE_EXCLUDE_MAIN` categories before `saved` is computed.

**Yearly Balance Sheet table** (`make_balance_sheet_card`, static, col-12)
- One row per year (newest first): Income, Spent, Invested, Net Gain, Gain %, Exp %, NetGain YoY %, Result (in-the-black / in-the-red).
- `Net Gain`, `Gain %`, `NetGain YoY %` use balance coloring (green ≥ 0, red < 0).
- Source: `data.pdf_BalanceSheet`.

**Major Category Spend by Year table** (`make_category_spend_card`, static, col-12)
- Pivot: years (rows, newest first) × major spend categories (`BALANCE_SHEET_MAJOR_CATEGORIES`, columns).
- Each cell shows the yearly CHF spend plus a YoY % delta, colored inverted (spend increase = red, decrease = green).
- Source: `data.pdf_CategorySpendPivot` / `data.pdf_CategorySpendYoY`.

---

## Budget / Forecasting

Route: `/budget-forecast` | Layout: `layout/budget_forecast.py` | Callbacks: `callbacks/budget_forecast.py`

See [`05-budget-forecasting.md`](05-budget-forecasting.md) for the forecasting model itself (seasonal pacing vs. lumpy median-rate routing, shrinkage). This section covers the page structure only.

**Set Yearly Budgets card** (`make_budget_input_card`, col-12)
- Year `dcc.Dropdown` (current year ± 2).
- One input row per active category (`make_budget_rows`): label, numeric CHF input, remove button. Active set starts from `cfg.budget_default_categories`, merged with any categories that already have a saved budget for the selected year (`resolve_active_categories`).
- Add-category dropdown + button appends a category to the active set (`dcc.Store(id="budget-active-categories")`).
- Save button persists all active rows via `data.save_budgets(year, budgets)`; status message shown in `"budget-save-status"`.

**Spend Progression & Year-End Forecast chart** (`budget-forecast-fig`, `get_fig_BudgetForecast`)
- One cumulative-spend line per active category: solid actual-to-date segment, dashed forecast segment to year-end.
- Rebuilt by `refresh_forecast` whenever the year, active categories, or save button changes.

**Budget vs Forecast by Category table** (`budget-table-container`, `make_budget_table_card`)
- Columns: Category, Budget, Spent, Forecast EOY, Budget used Now, Now Δ CHF/%, EOY Δ CHF/% (`BUDGET_TABLE_COLUMNS` in `layout/budget_forecast.py`).
- Δ columns use inverted balance coloring (over-budget = red, under-budget = green).
- Built by `forecast.build_budget_table` from `data.get_category_year_spend`, saved budgets, and the per-category forecast curves.

---

## Transport

Pipeline table: `src/.../tables/transport.py` → `dash_transport`, `dash_transport_heatmap`

**Transport expenses bar chart** (`fig_BarYearlyByCategory`)
- Grouped bar chart of annual spend per transport subcategory.
- Scope: `transaction_type == EXPENSE` and `category_main` in `{Car, Transport}`.
- Excluded: `Car / Purchase` (one-off car purchase distorts yearly totals). Defined in `config.TRANSPORT_EXCLUDE_SECOND`.
- Subcategory label uses `category_second` when present, falls back to `category_main`.

**Transport Heatmap** (`fig_HeatmapMonthly`)
- Monthly spend heatmap across years — rows = months, columns = years, colour = total CHF.
- Same base filter as above, plus additionally excludes `Car / Service & Repair` to remove large irregular service costs that would mask the regular commuting signal. Defined in `config.TRANSPORT_HEATMAP_EXCLUDE_SECOND`.

**Car expenses chart** (`fig-Car`, dynamic via callback)
- Grouped bar chart of Car-only expenses by subcategory, switchable between Yearly / Monthly frequency via a toggle button.
- Excluded subcategories: `Purchase`, `Car Rental`. Defined in `tables/car.py:CAR_EXCLUDE_SECOND`.
- Source: `dash_car`.

---

## M Cumulus Analytics

Route: `/m-cumulus` | Layout: `layout/groceries_detail.py` | Callbacks: `callbacks/groceries_detail.py`

Pipeline tables: `src/.../tables/groceries_detail.py` → `dash_groceries_cat`, `dash_groceries_health`, `dash_groceries_top_articles`

Data source: `groceries_use` (item-level Migros receipt data — article, category_main, category_detail, price_chf, discount_chf, date, location).

**KPI cards (top row)**

| Card | Value | Source |
|---|---|---|
| Monthly Spend | Sum of `price_chf` for the latest month | `pdf_GroceryItems` (computed in loader) |
| Avg. per Visit | Monthly spend ÷ receipt count | `pdf_GroceryItems` |
| Visits this Month | Count of distinct (date, location) pairs | `pdf_GroceryItems` |
| Health Score | Healthy Grocery Index for latest month (0–100) | `pdf_GroceryHealth` |

**Category Distribution donut** (`gd-fig-donut`, drill-down via callback)
- Default: spend share per `category_main`.
- Click a slice → drills into `category_detail` for that category.
- `← Back` button resets to top level.
- State tracked in `dcc.Store(id="gd-store-donut")`.

**Category Spend Over Time** (`gd-fig-cat`, month/year toggle)
- Stacked bar: `category_main` on the colour axis, period on X.
- Switchable between Monthly and Yearly frequency.
- Source: `dash_groceries_cat`.

**Healthy Grocery Index** (static)
- Line + markers: one score per month (0 = poor, 100 = optimal).
- Coloured background bands: green ≥ 70, yellow 40–70, red < 40.
- Scoring: basket composition weighted by category health value (defined in `groceries_detail.py:HEALTH_WEIGHTS`). Fresh Produce +1.0, Snacks & Sweets −1.0, etc.
- Source: `dash_groceries_health`.

**Spend Heatmap — Category x Month** (static)
- `category_main` on Y, month on X, colour = CHF spend.
- Annotated with CHF values per cell.
- Source: `dash_groceries_cat` (Monthly rows only).

**Top Bought Articles table** (static, top 15)
- Columns: Article, Category, Count, Total CHF, Avg CHF.
- Sorted by purchase count descending.
- Source: `dash_groceries_top_articles`.

---

## Dining

Pipeline table: `src/.../tables/food.py` → `dash_food`

**Food & Dining expenses chart** (`fig-Food`, dynamic via callback)
- Bar chart of spending on Groceries + Restaurant combined, switchable between Yearly / Monthly frequency.
- Scope: `category_main` in `{Groceries, Restaurant}`, `transaction_type == EXPENSE`.
- Groceries transactions are relabelled to subcategory `"Groceries"` before grouping (they have no meaningful second category in this context).

**Food & Dining per-visit box plot** (`fig_BoxFood`)
- Box-and-whisker distribution of individual transaction amounts per `category_second` (e.g. Dining, Fast Food, Cafe, Groceries).
- Shows spend variability per visit, not totals.
- Source: `pdf_Master`.

---

## Groceries

Pipeline table: `src/.../tables/groceries.py` → `dash_groceries`

**Grocery store expenses charts** (dual abs/pct, dynamic via callback)
- Two charts side by side: absolute CHF spend and percentage share per grocery merchant, switchable between Yearly / Monthly frequency.
- Scope: `category_main == Groceries`, `transaction_type == EXPENSE`.
- Only tracked merchants are included (others are too infrequent to be meaningful): `Coop, Migros, Lidl, Aldi, Denner, Avec, Migrolino, K-Kiosk, Spar`. Defined in `config.GROCERY_MERCHANTS_TRACKED`.
- Merchant names are normalised via substring matching before grouping (e.g. `"migrolino"` → `"Migrolino"`). Defined in `config.GROCERY_MERCHANT_NORMALIZE`.

**Grocery per-visit box plot** (`fig_BoxGrocery`)
- Box-and-whisker distribution of individual transaction amounts per grocery merchant.
- Source: `pdf_GroceryVisits` — same tracked-merchant filter as above.

---

## Sport

Pipeline table: `src/.../tables/sport.py` → `dash_sport`, `dash_sport_activities`

**KPI cards (top row)**

| Card | Value | Source |
|---|---|---|
| Avg / Year | All-time yearly sport spend averaged over distinct years | `data.v_SportAvgPerYear` |
| YTD *(label e.g. "YTD Jun 2026")* | Sum of monthly sport spend for the current year, up to the latest completed month | `data.v_SportCurrentYtd` / `data.s_SportYtdLabel` |
| vs Last Year (YTD) | % change vs. the same Jan–latest-month window last year (`None` → shown as `0.0`) | `data.v_SportPctVsLastYtd` |

**Sport expenses chart** (`fig-Sport`, dynamic via callback)
- Bar chart of sport spending by subcategory, switchable between Yearly / Monthly frequency.
- Scope: `category_main == Sport`, `transaction_type == EXPENSE`.
- Excluded subcategories (administrative / facility costs that are not personal sport activity): `Sports Facility`, `Unknown`, `Sports Administration`, `Sports services`. Defined in `config.SPORT_EXCLUDE_SECOND`.

**Activities per Year chart** (`fig_BarSportActivities`, static)
- Bar chart of yearly activity *counts* (not CHF) for Golf, Tennis, Padel.
- Golf counts only transactions priced between CHF 70–160 (`_GOLF_MIN`/`_GOLF_MAX` in `tables/sport.py`) to approximate a single green fee and exclude memberships/equipment.
- Source: `dash_sport_activities` (`data.pdf_SportActivities`).

---

## Vacation

Pipeline table: `src/.../tables/vacation.py` → `dash_vacation`

**Vacation expenses chart** (`fig_BarVacation`)
- Grouped bar chart of annual travel spend per subcategory (Flight, Hotel, Hostel, Apartment, Car Rental, Ticket Booking).
- Scope: `category_main == Travel`, `transaction_type == EXPENSE`.
- No further exclusions.

---

## Retail

Pipeline tables: `src/.../tables/retail.py` → `dash_retail`, `dash_retail_donut`, `dash_retail_top`

**KPI cards**

| Card | Value |
|---|---|
| Avg Yearly Retail Spend | All-time retail total ÷ number of years in data |
| Avg per Entry | All-time retail total ÷ number of transactions |
| Avg Transactions / Year | Transaction count ÷ number of years |

**Retail spend bar chart** (`fig-Retail`, dynamic via callback)
- Bar chart of retail spending by subcategory (Clothing, Electronics, Home Goods, etc.), switchable between Yearly / Monthly frequency.
- Scope: `category_main == Retail`, `transaction_type == EXPENSE`.

**Spend distribution donut chart** (`fig-Retail-Donut`)
- Donut chart of proportional spend per subcategory.
- Year dropdown allows filtering to a specific year or "All" time.
- Source: `dash_retail_donut`.

**Top 10 Retail Purchases table**
- The 10 largest individual retail transactions of all time, with date, merchant, subcategory, and amount.
- Source: `dash_retail_top`.

---

## Smart Table

No pipeline table — reads directly from `pdf_Master` at runtime.

An interactive transaction browser with the following filters:

| Filter | Behaviour |
|---|---|
| Date range | From year + optional month → To year + optional month. If month is omitted the full year boundary applies. |
| Category | Multi-select on `category_main`. |
| Subcategory | Multi-select on `category_second`, cascades from selected categories. |
| Merchant | Multi-select with search, cascades from selected category + subcategory. |
| Amount range | Range slider on `amount_CHF`. |
| Exclude non-expense transactions | Checkbox — when checked, drops the subcategories and main categories listed below. |

**Exclusion list when checkbox is active**

`category_second` excluded:
`Money Transfer`, `Donation`, `Brokerage`, `Taxes`, `Deposit`, `Rent`, `Payment Fees`, `Health Insurance`, `Transfer Fees`

`category_main` excluded:
`Insurance`, `Salary`

**Summary bar** shows transaction count, total CHF, and average CHF per transaction for the current filter state.

All filters operate on `transaction_type == EXPENSE` — income transactions are never shown.

---

## Investing

**Data source:** `positions.db / positions_use` — loaded once at startup by `PositionsLoader` (`app/data/loader_positions.py`). Does not use `DataStore` or any transaction tables.

`PositionsLoader` computes `invested_chf = value_chf - pnl_chf` (both columns are CHF-denominated, so this is always currency-correct regardless of the original position currency).

**KPI cards (top row, col-3 each)**

| Card | Formula |
|---|---|
| Total Invested | `SUM(invested_chf)` over latest snapshot |
| Total Value | `SUM(value_chf)` over latest snapshot |
| Unrealised P&L | `SUM(pnl_chf)` over latest snapshot |
| P&L % | `total_pnl_chf / total_invested × 100` |

**Row 2: Performers card (col-6) + Allocation donut (col-6)**

*Performers card* — shows the single best and worst position by `pnl_pct` from the most recent snapshot. A `dcc.RadioItems` toggle (`id="investing-year-scope"`, values `"year"` / `"all"`) filters snapshots to the current year or all time before picking best/worst. Each block shows: symbol (kpi-category), full name (kpi-subtext, if available), `pnl_pct` as `+/-X.XX %`, and `pnl_chf` as `+/-X,XXX.XX CHF`. Callback output: `id="investing-performers-content"`.

*Allocation donut* (`fig_allocation_donut`) — Plotly `go.Pie` with `hole=0.4` showing current portfolio allocation. Labels on slices ≥ 3% show `SYMBOL\nX.X%\nCHF X,XXX`; smaller slices show no text. Hover shows full name (from `name` column, falling back to symbol). Color sequence cycles through `_COLORS`.

**Row 3: Portfolio Progression (col-12)**

`fig_portfolio_progression` — two-trace scatter chart aggregated by date:
- *Invested* — dashed grey line (`invested_chf`), drawn first
- *Portfolio Value* — solid cyan line (`value_chf`), filled down to the invested line (`fill="tonexty"`, `fillcolor="rgba(25,211,243,0.15)"`)

Static figure, no callback. `height=350`, `margin={"l": 80}`, x-axis `tickformat="%Y-%m-%d"`.

**Row 4: Position Progression (col-12)**

A `dcc.Dropdown` (`id="investing-symbol-dropdown"`, `multi=True`) lists all symbols as `SYMBOL  —  Full Name` labels and defaults to all symbols. Filtered by the same year scope toggle as the Performers card. Two `dcc.Graph` below:

- *Value (CHF)* (`id="investing-pos-value"`, `style={"height": "300px"}`) — one line trace per symbol, CHF y-axis, `uirevision="pos-value"`, `margin={"l": 80}`
- *P&L (%)* (`id="investing-pos-pct"`, `style={"height": "300px"}`) — `pnl_pct × 100`, horizontal zero-line, `uirevision="pos-pct"`, `margin={"l": 65}`

Both charts use `mode="lines"` only (no markers).

**Callback registration**

`callbacks/investing.py` registers two callbacks via `register_callbacks(app, data, pos)`:

| Callback | Inputs | Output |
|---|---|---|
| `update_performers` | `"investing-year-scope"` | `"investing-performers-content"` children |
| `update_position_charts` | `"investing-symbol-dropdown"`, `"investing-year-scope"` | `"investing-pos-value"` figure, `"investing-pos-pct"` figure |

`register_all_callbacks` in `callbacks/__init__.py` passes `pos` (the `PositionsLoader` instance) to both the router and the investing callbacks. All other page callback modules keep the `register_callbacks(app, data)` signature unchanged.

---

## Data flow summary

```
Raw transactions (CSV/XLS)
    └── pipeline_ingestion  →  transactions.db (all transactions)
    │           └── pipeline_dash   →  pre-aggregated dash_* tables
    │                   └── app/data/loader.py  →  DataStore object
    │                           └── layout + callbacks  →  Dash UI (all pages except Investing)
    │
    └── pipeline_ingestion  →  positions.db (Swissquote position snapshots)
                    └── app/data/loader_positions.py  →  PositionsLoader object
                            └── layout/investing.py + callbacks/investing.py  →  Investing page
```

The `DataStore` object (loaded once at startup from `app/data/loader.py`) holds both the raw `pdf_Master` DataFrame and all pre-aggregated tables. Static pages use the pre-aggregated tables; the Smart Table queries `pdf_Master` directly per interaction.

`PositionsLoader` (loaded once at startup from `app/data/loader_positions.py`) holds the full `positions_use` history as a DataFrame plus latest-snapshot aggregates.
