# Budget Forecasting

The Budget / Forecast page (`/budget`) projects each budgeted category's **year-end
total** from the spend accrued so far, so it can be compared against the yearly budget
before the year is over. The forecasting logic lives in `app/data/forecast.py`; the data
it needs is prepared in `app/data/loader.py`; the page wires them together in
`callbacks/budget_forecast.py`.

The core problem is small-data time-series forecasting: a personal tracker holds only
1–4 years of history per category, spending is seasonal (Travel peaks in summer, Retail
at year-end), and some categories are dominated by a few large one-off transactions. Two
naive approaches both fail — a flat run-rate (`spend_to_date / fraction_of_year_elapsed`)
ignores seasonality, and a fancy model overfits 24–48 monthly data points and swings
wildly every time a new week arrives.

The model uses **two forecasting methods** and picks one per category:

1. **Seasonal pacing + shrinkage** — for *continuous* categories (a steady stream of
   spending: Restaurant, Groceries, Sport, Bar, Retail).
2. **Median monthly rate** — for *lumpy* categories (few but large transactions: Car,
   Travel, Transport, Healthcare).

A category is routed to method 2 when it is detected as lumpy (see
[Lumpiness detection](#lumpiness-detection)); otherwise it uses method 1.

---

## Tuning constants

All coefficients are collected here. Curve/level/lumpiness constants live in
`app/data/forecast.py`; the two that a user might plausibly want to change live in the
`config` dataclass (`app/config.py`).

| Constant                          | Value    | Where           | Meaning                                                                                                                        |
| --------------------------------- | -------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `forecast_shrinkage_k`          | `0.1`  | `config`      | Shrinkage`k` in the year-end blend `w = elapsed / (elapsed + k)`. Lower ⇒ current-year pace takes over sooner.            |
| `forecast_lumpy_lookback_years` | `2`    | `config`      | Years of monthly history used to detect lumpiness and compute the median rate.                                                 |
| `DEFAULT_SHRINKAGE_K`           | `0.1`  | `forecast.py` | Library fallback for`k` when a caller doesn't pass the config value.                                                         |
| `_CURVE_LINEAR_REG`             | `0.35` | `forecast.py` | How far the multi-year pacing curve is pulled toward a flat/linear curve. A single prior year is regularized harder (`0.5`). |
| `_CURVE_MIN_YEAR_FRAC`          | `0.2`  | `forecast.py` | Years whose total is below this fraction of the median year total are ignored when learning the seasonal shape.                |
| `_LEVEL_RECENT_YEARS`           | `2`    | `forecast.py` | Number of most-recent prior years averaged into the history level anchor.                                                      |
| `_LUMPY_MEAN_MEDIAN_RATIO`      | `1.3`  | `forecast.py` | A category is lumpy when its mean monthly spend exceeds this multiple of its median monthly spend.                             |

---

## Method 1 — Seasonal pacing + shrinkage (continuous categories)

### Step 1: seasonal pacing curve — `seasonal_pacing_curve`

Learns *when* in the year a category's spending typically happens, as a cumulative-share
curve `cum_share(year_fraction)` on a grid. The resolution is fixed to **weekly**
(`config.forecast_freq_default`; the daily/weekly/monthly selector was removed), though the
curve functions still accept `"D" | "W" | "M"`. For each prior year it normalizes its
cumulative spend to
its own total (so magnitude drops out and only the shape remains), then combines years
with three robustness guards:

- **Drop negligible years.** Years whose total is below `_CURVE_MIN_YEAR_FRAC` (0.2) of
  the median year total are discarded — a year where you spent 346 CHF total has a
  meaningless "shape" that would otherwise distort the curve. (If dropping would empty
  the set, all years are kept.)
- **Median across the kept years** (not mean). Median is robust to a lone spike year: a
  category bought in one month of one year (e.g. a car in August 2023) can't drag the
  whole curve back-loaded, because the other normal years out-vote it.
- **Regularize toward linear.** The learned curve is blended a fraction
  `_CURVE_LINEAR_REG` (0.35) toward a flat curve — with only 2–4 noisy years of personal
  data we hedge the learned seasonality ~a third of the way to "flat". A single prior
  year is regularized harder (0.5). This bounds how far a sparse or lopsided history can
  amplify the run-rate. The curve is forced monotonic (`np.maximum.accumulate`) and
  pinned to end at `1.0`.

`pacing_fraction(curve, as_of)` interpolates the curve at `as_of`'s day-of-year to give
the **seasonal share** — the fraction of the year's spend that has typically occurred by
now (floored at `1/365` to avoid a zero denominator on Jan 1).

### Step 2: pace forecast

The pure seasonal projection is `naive_pace = spend_to_date / seasonal_share`. This
already beats a flat run-rate because it knows, e.g., that only ~30 % of annual Retail
spend happens by June, not 50 %.

### Step 3: shrinkage toward the historical level — `forecast_year_end`

Early in the year `naive_pace` divides by a small number and is noisy; one big January
purchase would swing the whole-year estimate. So we shrink it toward a historical level.

**History level anchor** — `historical_annual_level`: the mean of the most recent
`_LEVEL_RECENT_YEARS` (2) prior-year totals. Using recent years (not a median over all
history) keeps the anchor tracking the *current* spending level rather than lagging on a
long, growing history. Zero-spend years are ignored; returns `None` (no shrinkage) for a
brand-new category with no history.

**The blend** is *geometric* (log-space) and weighted by *elapsed calendar time*:

```
forecast = pace ** w  *  prior_level ** (1 - w)
w        = elapsed_time / (elapsed_time + k)          # elapsed_time = day_of_year / days_in_year
```

Two deliberate choices here:

- **Weight on elapsed time, not seasonal share.** For a back-loaded category the seasonal
  share stays tiny until its season arrives; weighting on it would keep history dominant
  through exactly the months where you want the model to notice you're spending little
  this year. Elapsed calendar time decays history's influence steadily for *every*
  category — by ~June `w ≈ 0.8` (with `k = 0.1`), so the current-year pace dominates.
  Only the first weeks lean on history, for noise control.
- **Geometric (multiplicative) blend, not additive.** Spending levels are multiplicative,
  so history should be a mild *proportional* nudge, not an absolute pull. An additive
  blend with even a small weight on a large historical level adds a big absolute chunk
  and drags a genuinely low year up toward last year's total; the geometric blend does
  not. When the current pace agrees with history the blend leaves it essentially
  untouched.

Edge cases: on Jan 1 (`day_of_year <= 1`) the forecast is `prior_level` (or
`spend_to_date` if there is no history); with nothing spent yet (`naive_pace <= 0`) it
falls back to `(1 - w) * prior_level`.

---

## Method 2 — Median monthly rate (lumpy categories)

For categories dominated by a few large transactions, the pace/run-rate approach
over-projects badly: one expensive car repair in the current YTD, divided by a small
seasonal share, extrapolates into a phantom year-end blow-up. The fix is to project the
**remaining** year from a spike-resistant rate.

### Lumpiness detection — `is_lumpy_category`

Uses `loader.get_category_monthly_totals`, which returns per-category **zero-filled
monthly totals** for the last `forecast_lumpy_lookback_years` (2) years (every month of
each included year contributes, so medians reflect quiet months too).

A category is lumpy when:

```
mean(monthly_totals) > _LUMPY_MEAN_MEDIAN_RATIO * median(monthly_totals)     # 1.3
```

i.e. a handful of big months pull the mean well above the typical month. A category whose
median monthly spend is `0` (spend sits in a minority of months) is lumpy by definition.
Continuous categories have mean ≈ median and are *not* lumpy.

### The forecast — `forecast_year_end_lumpy`

```
forecast = spend_to_date + median_monthly_rate * months_remaining
```

where `median_monthly_rate` is `robust_monthly_rate` (the median of the lookback monthly
totals) and `months_remaining = 12 * (1 - day_of_year / days_in_year)`.

Big purchases **already made stay counted** — they are part of `spend_to_date` — but the
rest of the year is projected at the *typical* month's rate, so a one-off does not inflate
the whole forecast. This is symmetric: a category running *below* its typical rate in H1
(e.g. a quiet Healthcare year) reverts *up* toward the median rate for the remaining
months, which is the honest behavior for unpredictable bills.

On the chart, lumpy categories draw a **straight forecast line** at the median rate from
`as_of` onward (`build_forecast_line` with `monthly_rate` set), instead of following the
seasonal curve.

---

## Category classification (real data, 2-year lookback)

Lumpiness ratio = mean ÷ median of monthly totals. Threshold `1.3`.

> This table is a **snapshot of current real data**, not a hardcoded mapping. The class
> is computed on the fly for *any* selected category from its own last-2-years monthly
> history (`is_lumpy_category`) — no category names appear in the code. A new category is
> classified the moment it's selected, and a category is re-classified automatically as
> its recent history shifts. Ratios below will drift as more data arrives.

| Category   | ratio         | Class           | Method          |
| ---------- | ------------- | --------------- | --------------- |
| Sport      | 0.98          | continuous      | seasonal pacing |
| Groceries  | 1.05          | continuous      | seasonal pacing |
| Restaurant | 1.11          | continuous      | seasonal pacing |
| Retail     | 1.14          | continuous      | seasonal pacing |
| Bar        | 1.17          | continuous      | seasonal pacing |
| Car        | 1.36          | **lumpy** | median rate     |
| Healthcare | 1.36          | **lumpy** | median rate     |
| Transport  | 1.76          | **lumpy** | median rate     |
| Clothing   | 2.43          | **lumpy** | median rate     |
| Travel     | 3.35          | **lumpy** | median rate     |
| Car Rental | ∞ (median 0) | **lumpy** | median rate     |

**Note on Retail (ratio 1.14 ⇒ continuous):** Retail *is* historically regular, so it uses seasonal pacing. Its occasional big purchases (a new
sofa) are *within-year* events, and the detector reads *historical* lumpiness — so a
one-off in an otherwise-steady category is not caught. Handling that would require
dampening spikes in the **current** year's run-rate (winsorizing this year's monthly
spend), which is not currently implemented.

---

## Data layer — `loader.py`

| Method                                                      | Returns                                                                             | Used for                                        |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------- | ----------------------------------------------- |
| `get_category_year_spend(year)`                           | per-category YTD EXPENSE total                                                      | current spend-to-date in the budget table       |
| `get_category_cumulative(year, cats, freq)`               | long-format cumulative spend per category at`freq`                                | the "actual" segment of each chart line         |
| `get_category_period_history(cats, before_year, freq)`    | per-period spend for all prior years (`category, year, year_fraction, spend_chf`) | building the pacing curve and the history level |
| `get_category_monthly_totals(cats, before_year, n_years)` | per-category list of zero-filled monthly totals for the last`n_years`             | lumpiness detection + median rate               |

All amounts are EXPENSE-only and cover both `category_main` and `category_second` levels.

---

## Orchestration — `callbacks/budget_forecast.py`

`refresh_forecast` runs on any change to the year, active categories, or a budget save
(the resolution is fixed to weekly). For each selected category it:

1. builds the seasonal pacing curve (`seasonal_pacing_curve`),
2. computes the history level anchor (`historical_annual_level`),
3. fetches monthly totals and, if the category `is_lumpy_category`, computes its
   `robust_monthly_rate`,
4. builds the chart line (`build_forecast_line`) — passing `monthly_rate` for lumpy
   categories so the forecast segment is linear, or `prior_level` + `k` for continuous
   ones so it follows the pacing curve and shrinks toward history.

It then builds the **budget vs forecast table** (`build_budget_table`). Columns, in order:
`Category | Budget | Spent | Forecast EOY | Budget used Now | Now Δ CHF | Now Δ % | EOY Δ CHF | EOY Δ %` — the three magnitudes (target, spent-so-far, projected year-end) sit
together up front so the differences are visible at a glance, then the `now` block (its
target + two deltas) and the `end-of-year` delta pair.

Two comparisons per category, against different references:

- **Now** — are you on pace *today*? The pace target `Budget used Now`
  (`pace_budget = budget × elapsed_year_fraction`) is a **linear** pro-ration of the
  budget: for a 3000 budget on June 1 you'd expect to have spent ~5/12 ≈ 1250, assuming
  even spending and *ignoring* seasonality (unlike the forecast, this is about "am I on
  track", for which a flat monthly target is the intuitive yardstick). `Now Δ CHF = spend − pace_budget`, and `Now Δ %` is that delta **relative to the pace target**
  (`over_under_now_chf / pace_budget`), i.e. how far off pace you are right now — not a
  fraction of the annual budget.
- **End of year** — will you land over budget? `EOY Δ CHF = forecast − budget` and `EOY Δ % = over_under_eoy_chf / budget` (relative to the full annual budget).

Both `Δ %` columns divide by zero safely (a zero budget / pace target yields `0.0`).

---

## Back-testing

`scripts/forecast_model_analysis.py` (a jupytext *percent* notebook) back-tests the
production model against candidate baselines (flat run-rate, last-year ratio,
Holt-Winters) by projecting the year-end total from partial-year data at several
checkpoints and scoring absolute percentage error. It imports the production functions
directly, so it always reflects the live model; `forecast_year_end` with `prior_level = None` gives the pure pace forecast used as one of the baselines. Open it as a notebook and
change `CATEGORY` / `FREQ` to explore a category.
