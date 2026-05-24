# Dashboard Structure

The dashboard is a Plotly Dash application (`app/`). Each page has a `layout/` module that builds the static HTML structure and a `callbacks/` module that wires up the interactive elements. Data is served from pre-aggregated SQLite tables produced by the `pipeline_dash` pipeline in `src/`.

---

## Page overview

| Page | Route | Layout file | Callbacks file |
|---|---|---|---|
| Home | `/` | `layout/home.py` | `callbacks/home.py` |
| Transport | `/transport` | `layout/transport.py` | `callbacks/transport.py` |
| Dining | `/food` | `layout/food.py` | — |
| Groceries | `/groceries` | `layout/groceries.py` | — |
| Sport | `/sport` | `layout/sport.py` | `callbacks/sport.py` |
| Vacation | `/vacation` | `layout/vacation.py` | — |
| Retail | `/retail` | `layout/retail.py` | `callbacks/retail.py` |
| Smart Table | `/smarttable` | `layout/smarttable.py` | `callbacks/smarttable.py` |

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

Pipeline table: `src/.../tables/sport.py` → `dash_sport`

**Sport expenses chart** (`fig-Sport`, dynamic via callback)
- Bar chart of sport spending by subcategory, switchable between Yearly / Monthly frequency.
- Scope: `category_main == Sport`, `transaction_type == EXPENSE`.
- Excluded subcategories (administrative / facility costs that are not personal sport activity): `Sports Facility`, `Unknown`, `Sports Administration`, `Sports services`. Defined in `config.SPORT_EXCLUDE_SECOND`.

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

## Data flow summary

```
Raw transactions (CSV)
    └── pipeline_ingestion  →  master SQLite table (all transactions)
            └── pipeline_dash   →  pre-aggregated dash_* tables
                    └── app/data/loader.py  →  DataStore object
                            └── layout + callbacks  →  Dash UI
```

The `DataStore` object (loaded once at startup from `app/data/loader.py`) holds both the raw `pdf_Master` DataFrame and all pre-aggregated tables. Static pages use the pre-aggregated tables; the Smart Table queries `pdf_Master` directly per interaction.
