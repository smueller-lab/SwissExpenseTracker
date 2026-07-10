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
| `forecast_shrinkage_k`          | `0.3`  | `config`      | Shrinkage`k` in the year-end blend `w = elapsed / (elapsed + k)`. Lower ⇒ current-year pace takes over sooner; raise it to lean on the calmer historical level for longer.            |
| `forecast_lumpy_lookback_years` | `2`    | `config`      | **Prior** years of monthly history used to detect lumpiness and compute the median rate. The current year's completed months are always added on top of this.                                                 |
| `DEFAULT_SHRINKAGE_K`           | `0.1`  | `forecast.py` | Library fallback for`k` when a caller doesn't pass the config value (used by the back-test notebook, not the app).                                                         |
| `_CURVE_LINEAR_REG`             | `0.35` | `forecast.py` | How far the multi-year pacing curve is pulled toward a flat/linear curve. A single prior year is regularized harder (`0.5`). |
| `_CURVE_MIN_YEAR_FRAC`          | `0.2`  | `forecast.py` | Years whose total is below this fraction of the median year total are ignored when learning the seasonal shape.                |
| `_LEVEL_RECENT_YEARS`           | `2`    | `forecast.py` | Number of most-recent prior years averaged into the history level anchor.                                                      |
| `_LUMPY_MEAN_MEDIAN_RATIO`      | `1.3`  | `forecast.py` | A category is lumpy when its mean monthly spend exceeds this multiple of its median monthly spend.                             |
| `_CONTINUOUS_MONTH_SPIKE_CAP_MULTIPLE` | `2.0` | `forecast.py` | For a *continuous* category, a completed month this year spending over this multiple of the median month is a one-off; the excess is held out of the pace calc (see [Spike flattening](#spike-flattening-for-continuous-categories)). |

---

## Method 1 — Seasonal pacing + shrinkage (continuous categories)

### Step 1: seasonal pacing curve — `seasonal_pacing_curve`

Learns *when* in the year a category's spending typically happens, as a cumulative-share
curve `cum_share(year_fraction)` on a grid. The resolution is fixed to **weekly**
(`config.forecast_freq_default`), though the curve functions still accept `"D" | "W" | "M"`. For each prior year it normalizes its cumulative spend to its own total (so magnitude drops out and only the shape remains), then combines years with three robustness guards:

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

### Spike flattening for continuous categories — `spike_excess_this_year`

A category can be historically *continuous* (mean ≈ median over 1–4 years) and still take
one unusually large transaction in the current year — a new sofa in an otherwise-steady
Retail habit. Dividing that inflated `spend_to_date` by a small seasonal share (early in
the year, `seasonal_share` can be ~0.35) extrapolates the one-off as if it recurred every
remaining period, blowing the forecast up far past both the naive run-rate and history.

`spike_excess_this_year(this_year_months, median_rate)` sums, over the current year's
*completed* months, how much each exceeds `_CONTINUOUS_MONTH_SPIKE_CAP_MULTIPLE` (2.0)
times the category's own median monthly rate (`robust_monthly_rate`, the same statistic
`is_lumpy_category` uses — already robust to the very spikes being flagged). This total is
`spike_excess`.

`forecast_year_end` (below) holds `spike_excess` out of `spend_to_date` before dividing by
the seasonal share — so the one-off isn't treated as a recurring rate — then adds it back
once, unshrunk, at the end, since that money was still genuinely spent. Only applied to
non-lumpy categories; a lumpy category already forecasts off the median rate (Method 2),
which doesn't have this failure mode.

### Step 3: shrinkage toward the historical level — `forecast_year_end`

Early in the year `naive_pace` divides by a small number and is noisy; one big January
purchase would swing the whole-year estimate. So we shrink it toward a historical level.

**History level anchor** — `historical_annual_level`: the mean of the most recent
`_LEVEL_RECENT_YEARS` (2) prior-year totals. Using recent years (not a median over all
history) keeps the anchor tracking the *current* spending level rather than lagging on a
long, growing history. Zero-spend years are ignored; returns `None` (no shrinkage) for a
brand-new category with no history.

**The blend** is *geometric* (log-space) and weighted by *elapsed calendar time*, computed
on `naive_pace` after `spike_excess` has been held out (see above), then `spike_excess` is
added back to the result:

```
baseline_pace = (spend_to_date - spike_excess) / seasonal_share
forecast      = baseline_pace ** w  *  prior_level ** (1 - w)  +  spike_excess
w             = elapsed_time / (elapsed_time + k)          # elapsed_time = day_of_year / days_in_year
```

Two deliberate choices here:

- **Weight on elapsed time, not seasonal share.** For a back-loaded category the seasonal
  share stays tiny until its season arrives; weighting on it would keep history dominant
  through exactly the months where you want the model to notice you're spending little
  this year. Elapsed calendar time decays history's influence steadily for *every*
  category — by ~June `w ≈ 0.64` (with `k = 0.3`, the app default), so the current-year
  pace still dominates the blend but history has more say than a smaller `k` would give it.
  `k` is deliberately not tiny: `baseline_pace` is the more volatile side of the blend (a
  seasonal-share division, sensitive to *this* year's timing) while `prior_level` is a
  calmer multi-year average, so leaning on history for longer damps categories whose
  current-year pace is running unusually high even after spike flattening.
- **Geometric (multiplicative) blend, not additive.** Spending levels are multiplicative,
  so history should be a mild *proportional* nudge, not an absolute pull. An additive
  blend with even a small weight on a large historical level adds a big absolute chunk
  and drags a genuinely low year up toward last year's total; the geometric blend does
  not. When the current pace agrees with history the blend leaves it essentially
  untouched.

Edge cases: on Jan 1 (`day_of_year <= 1`) the forecast is `prior_level` (or
`spend_to_date` if there is no history); with nothing spent yet (`baseline_pace <= 0`) it
falls back to `(1 - w) * prior_level + spike_excess`.

Raising `k` is a global knob, not a per-category one: for a category currently running
*below* its historical level (e.g. a quiet Restaurant year), the same increase pulls the
forecast *up* toward history rather than down. There's no signal in `forecast_year_end`
distinguishing "pace is inflated by a spike" from "pace is genuinely lower this year" —
both just look like "current pace disagrees with history."

### Anchoring the forecast line — `build_forecast_line`

The year-end scalar from `forecast_year_end` only tells you the *total*; the chart needs a
full trajectory. The naive approach — scale the raw curve by the total,
`fc_cum(t) = year_end_fc * cum_share(t)` — only lands exactly on `spend_to_date` at `as_of`
when `year_end_fc` equals the unshrunk `naive_pace` (because `cum_share(as_of) *
naive_pace == spend_to_date` by construction). As soon as `year_end_fc` is pulled off that
value — by the history blend, or by adding `spike_excess` back — the scaled curve no longer
passes through the real `spend_to_date` at the seam, which can produce a **visible jump at
the boundary, including downward** for cumulative spend, which is otherwise never supposed
to decrease.

Instead the forecast segment is anchored explicitly: it starts at `spend_to_date` and
distributes `year_end_fc - spend_to_date` across the remaining periods in proportion to the
curve's *remaining* share:

```
progress(t) = (cum_share(t) - cum_share(as_of)) / (1 - cum_share(as_of))   # clipped to >= 0
fc_cum(t)   = spend_to_date + (year_end_fc - spend_to_date) * progress(t)
```

This guarantees `fc_cum(as_of) == spend_to_date` (no seam jump) and
`fc_cum(year_end) == year_end_fc` exactly, and is monotonic whenever `year_end_fc >=
spend_to_date` — which is enforced with a floor (`year_end_fc = max(year_end_fc,
spend_to_date)`), since a forecast can't imply the year ends with less spend than has
already happened.

---

## Method 2 — Median monthly rate (lumpy categories)

For categories dominated by a few large transactions, the pace/run-rate approach
over-projects badly: one expensive car repair in the current YTD, divided by a small
seasonal share, extrapolates into a phantom year-end blow-up. The fix is to project the
**remaining** year from a spike-resistant rate.

### Lumpiness detection — `is_lumpy_category`

Uses `loader.get_category_monthly_totals(categories, as_of, n_years)`, which returns
per-category monthly totals for the `n_years` (`forecast_lumpy_lookback_years`, 2) prior
years — each **zero-filled** across all 12 months, so medians reflect quiet months too —
**plus the current year through its last fully completed month** (the in-progress month is
excluded so it can't drag the median toward zero before it's actually over). Including the
current year means a category's classification and median rate reflect what's actually
happening this year, not just history.

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

### Wobble, not a straight ramp — `_jitter_monthly_path`

A dead-straight line from `as_of` to the year-end total looked artificial next to the
jagged actual line, so lumpy categories now draw a **wobbled** forecast instead of a flat
ramp:

- `robust_monthly_deviation` — the median absolute deviation of the lookback monthly
  totals, scaled to std-equivalent units (`* 1.4826`), i.e. the same outlier-resistant
  statistic as `robust_monthly_rate` applied to spread instead of center.
- Each future period's increment gets **lognormal noise** sized by the coefficient of
  variation `monthly_deviation / monthly_rate` (capped at `1.5` so a noisy/near-zero rate
  can't produce wild swings), then all increments are **rescaled** so they still sum to
  exactly the `forecast_year_end_lumpy` total — the wobble changes the *path*, never the
  *year-end number* shown in the budget table.
- Increments are never negative, so the cumulative line still never dips.
- Noise is seeded from `f"{category}-{year}"` (`_deterministic_seed`, via
  `hashlib.blake2b`) — reproducible across reloads of the same category/year instead of
  reshuffling on every refresh.

On the chart, lumpy categories draw this wobbled line from `as_of` onward
(`build_forecast_line` with `monthly_rate`, `monthly_deviation`, and `seed_key` set),
instead of following the seasonal curve.

---

## Category classification (real data, 2-year lookback + current year)

Lumpiness ratio = mean ÷ median of monthly totals. Threshold `1.3`.

> This table is a **snapshot of current real data**, not a hardcoded mapping. The class
> is computed on the fly for *any* selected category from its own monthly history
> (`is_lumpy_category`, now 2 prior years + the current year's completed months) — no
> category names appear in the code. A new category is classified the moment it's
> selected, and a category is re-classified automatically as its recent history shifts.
> Ratios below will drift as more data arrives.

| Category   | ratio         | Class           | Method          |
| ---------- | ------------- | --------------- | --------------- |
| Sport      | 1.04          | continuous      | seasonal pacing |
| Groceries  | 1.06          | continuous      | seasonal pacing |
| Restaurant | 1.08          | continuous      | seasonal pacing |
| Bar        | 1.16          | continuous      | seasonal pacing |
| Retail     | 1.20          | continuous      | seasonal pacing |
| Healthcare | 1.41          | **lumpy** | median rate     |
| Car        | 1.72          | **lumpy** | median rate     |
| Transport  | 2.06          | **lumpy** | median rate     |
| Travel     | 6.39          | **lumpy** | median rate     |
| Clothing   | ∞ (median 0) | **lumpy** | median rate     |
| Car Rental | ∞ (median 0) | **lumpy** | median rate     |

**Note on Retail (ratio 1.20 ⇒ continuous):** Retail *is* historically regular enough to
stay under the `1.3` threshold, so it uses seasonal pacing. It still takes occasional big
purchases (a new sofa) within an otherwise-steady year; those are now caught by
[spike flattening](#spike-flattening-for-continuous-categories) instead of by reclassifying
the whole category as lumpy — which would also flatten its normal, non-spike months to a
median-rate ramp and lose the seasonal shape.

---

## Data layer — `loader.py`

| Method                                                      | Returns                                                                             | Used for                                        |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------- | ----------------------------------------------- |
| `get_category_year_spend(year)`                           | per-category YTD EXPENSE total                                                      | current spend-to-date in the budget table       |
| `get_category_cumulative(year, cats, freq)`               | long-format cumulative spend per category at`freq`                                | the "actual" segment of each chart line         |
| `get_category_period_history(cats, before_year, freq)`    | per-period spend for all prior years (`category, year, year_fraction, spend_chf`) | building the pacing curve and the history level |
| `get_category_monthly_totals(cats, as_of, n_years)` | per-category list of monthly totals: `n_years` prior years (zero-filled, 12 months each) plus `as_of`'s year through its last completed month             | lumpiness detection, median rate, spike flattening               |

All amounts are EXPENSE-only and cover both `category_main` and `category_second` levels.

---

## Orchestration — `callbacks/budget_forecast.py`

`refresh_forecast` runs on any change to the year, active categories, or a budget save
(the resolution is fixed to weekly). For each selected category it:

1. builds the seasonal pacing curve (`seasonal_pacing_curve`),
2. computes the history level anchor (`historical_annual_level`),
3. fetches monthly totals (now including the current year's completed months) and, if the
   category `is_lumpy_category`, computes its `robust_monthly_rate` and
   `robust_monthly_deviation` (for the wobble); otherwise computes `spike_excess_this_year`
   from this year's completed months,
4. builds the chart line (`build_forecast_line`) — passing `monthly_rate` +
   `monthly_deviation` + a `seed_key` (`f"{category}-{year}"`) for lumpy categories so the
   forecast segment wobbles around the median rate, or `prior_level` + `k` +
   `spike_excess` for continuous ones so it follows the pacing curve, shrinks toward
   history, and flattens any one-off month.

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
