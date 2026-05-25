# 01 — Agentic Enrichment Pipeline

## Overview

The agentic pipeline enriches every transaction with structured merchant metadata
(canonical name, main category, sub-category, city) using a combination of a
ChromaDB vector-store cache and two OpenAI SDK agents backed by live web search.

```
transactions_rfn (status=pending)
        │
        ▼
  MerchantManager.run()
        │
        ├─► is_person?  ─── YES ──► hardcoded FRIEND / FRIEND_SUPPORT_PAYMENT
        │
        ├─► vector-store cache hit?  ─── YES ──► use cached MerchantMetaData
        │                                        skip web search & LLM calls
        │
        └─► NO ──► [summary_agent] web search → merchant summary
                         │
                         ▼
                   [metadata_agent] summary → MerchantMetaData (name, categories, city)
                         │
                         ▼
                   save to ChromaDB vector store (for future cache hits)
                         │
                         ▼
                   save to merchant_metadata_raw (SQLite)
                         │
                         ▼
                   mark transaction enriched in transactions_rfn
```

---

## Entry Points

| File | Purpose |
|------|---------|
| `src/swiss_exp_tracker/pipeline.py` | Top-level runner — calls all four stages |
| `src/swiss_exp_tracker/pipeline_agentic/pipeline.py` | `load_pending_transactions()` + `run_all_transactions()` |
| `src/swiss_exp_tracker/pipeline_agentic/merchant_manager.py` | `MerchantManager.run()` — orchestrates the per-transaction flow |

---

## Agents

### summary_agent — Web Search Agent
**File:** `pipeline_agentic/agents_/agent_summary.py`

Searches the web for information about a merchant and returns a plain-text summary.
It is given a `search_web` function tool that handles the full provider fallback chain.

**Model:** `gpt-4.1-mini`

**Output:** `SearchToolResult` (Pydantic `BaseModel`)
```python
class SearchToolResult(BaseModel):
    summary: str          # free-text merchant description
    tool_used: WebSearchTool
```

---

### metadata_agent — Categorisation Agent
**File:** `pipeline_agentic/agents_/agent_metadata.py`

Receives the merchant name, booking text, and web-search summary. Returns a
structured `MerchantMetaData` object with canonical name, `CategoryMain`,
`CategorySecond`, and city.

**Model:** `gpt-4.1-mini`

**Output guardrail:** rejects results where `category_main` is `None` (forces
the agent to retry rather than return an uncategorised result).

**Output:** `MerchantMetaData` (Pydantic `BaseModel`)
```python
class MerchantMetaData(BaseModel):
    name: str
    category_main: CategoryMain | None
    category_second: str | None
    city: str | None
```

---

## Categories

Defined as `StrEnum` in `pipeline_agentic/data_models/merchant.py`.

### CategoryMain
| Value | Description |
|-------|-------------|
| `Sport` | Sports activities, clubs, memberships |
| `Entertainment` | Streaming, concerts, cinema, spectator events |
| `Telecommunication` | Mobile, internet, TV subscriptions |
| `Restaurant` | All food & drink consumption (eat-in, takeaway, bar, café) |
| `Healthcare` | Insurance, pharmacy, dentist, doctor |
| `Government` | Taxes, government fees |
| `Retail` | Clothing, electronics, sporting-goods stores |
| `Groceries` | Migros, Coop, Aldi, LIDL, Denner, migrolino |
| `Salary` | Employer salary payments |
| `Housing` | Rent, utilities |
| `Car` | Fuel, parking, service, car purchase |
| `Transport` | Train, bus, SBB, tram — not flights |
| `Travel` | Flights, hotels, Airbnb, car rental |
| `Insurance` | All insurance types |
| `Education` | Online courses, Udemy |
| `Payment Services` | Credit card fees, payment processors |
| `Investing` | Revolut, TrueWealth, brokerage |
| `Postal Services` | Swiss Post, shipping |
| `Friend` | Person-to-person payments identified by name or phone number |

### CategorySecond (selected)
Sub-categories refine `CategoryMain`. Examples:
- Sport → `Tennis | Golf | Padel | Bike | Fitness | Running | Swimming`
- Entertainment → `Music Streaming | Events & Concerts | Cinema | Theatre | TV & Streaming | Dating | Amusement Park`
- Restaurant → `Dining | Fast Food | Cafe | Bar | Food Delivery`
- Transport → `Train | Bus | Tram | Taxi | E-Scooter`
- Car → `Fueling | Parking | Car Washing | Car Service | Car Rental`

Full list: `pipeline_agentic/data_models/merchant.py → CategorySecond`.

---

## Web-Search Fallback Chain

Implemented in `pipeline_agentic/agents_/agent_summary.py → search_web()`.

Priority order (free tiers exhausted before moving to pay-per-use):

| Priority | Provider | Cost model |
|----------|----------|-----------|
| 1 | **Tavily** free tier | 1 000 requests/month |
| 2 | **Brave Search** free tier | 1 000 requests/month |
| 3 | **Scrape.do** free tier | 1 000 requests/month |
| 4 | **Exa** free tier | 500 credits/month (10 credits/result) |
| 5 | Tavily pay-per-use | per request |
| 6 | Exa pay-per-use | 10 credits/result (last resort) |

Credit consumption is persisted per-provider per-month in `api_usage` (SQLite).
Load/save helpers live in `pipeline_agentic/libs.py`.

---

## Vector-Store Cache

**Implementation:** ChromaDB (`PersistentClient`) with cosine-similarity embeddings.
**Location:** `merchant_vector_store/`
**Class:** `MerchantStore` (`pipeline_agentic/merchant_store.py`)

A merchant is looked up by name with a similarity threshold. On a cache hit the
full `MerchantMetaData` is reconstructed from the stored ChromaDB metadata —
no web search or LLM call is made. On a cache miss the enriched result is saved
back to the store for future hits.

---

## Database Tables

All tables live in `database/transactions.db`.

### `merchant_metadata_raw`
Raw output of the agentic pipeline — one row per enrichment run per transaction
(multiple rows possible for the same `zkb_reference` if re-enriched).

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER PK | |
| `created_at` | TEXT | ISO timestamp |
| `zkb_reference` | TEXT | FK to `transactions_rfn.reference` |
| `matched_merchant` | TEXT | canonical merchant name |
| `cache_hit` | INTEGER | 1 = vector-store hit, 0 = web search |
| `similarity` | REAL | cosine similarity score (cache hit only) |
| `search_tool` | TEXT | which web-search provider was used |
| `category_main` | TEXT | `CategoryMain` value |
| `category_second` | TEXT | `CategorySecond` value (nullable) |
| `city` | TEXT | city extracted by metadata_agent |

### `merchant_metadata_rfn`
Cleaned/corrected copy of the latest raw row per `zkb_reference`.
Written by `run_post_clean()` (`clean_pipeline_output.py`).

Upsert logic: if a newer raw row exists for an already-present reference, the
old rfn row is deleted and replaced.

Manual corrections are defined at the top of `clean_pipeline_output.py`:
- `CORRECTIONS` — exact match on `matched_merchant`
- `CONTAINMENT_CORRECTIONS` — substring match (used for workplace salary entries)

### `transactions_use`
Final analysis-ready table. JOIN of `transactions_rfn` + `merchant_metadata_rfn`
on `reference = zkb_reference`. Only `enrichment_status = 'enriched'` rows are
included. Incremental — skips references already present.

Safe to drop and rebuild at any time; no source data is stored here.

### `api_usage`
Credit tracking per provider per month.

| Column | Notes |
|--------|-------|
| `provider` | `tavily`, `brave_search`, `scrape_do`, `exa` |
| `period` | `YYYY-MM` |
| `used` | credits consumed this period |
| `credit_limit` | monthly cap |
| `updated_at` | last update timestamp |

---

## Concurrency

Web search is the dominant bottleneck — each external lookup can take several
seconds — so transactions are processed concurrently rather than one at a time.

`run_all_transactions()` uses:
- `asyncio.Semaphore(5)` — max 5 transactions in flight simultaneously
- Per-merchant `asyncio.Lock` — ensures two transactions with the same merchant
  name never run concurrently (avoids duplicate vector-store writes and redundant
  web searches)

Pending transactions are sorted so that **unique merchants are processed first**.
This maximises the chance that by the time a second transaction for the same merchant
is picked up, the first has already completed and written a cache entry to the vector
store — turning what would have been a web search into a cheap cache hit.

---

## Data Models

All models are Pydantic `BaseModel` subclasses in `pipeline_agentic/data_models/`.

| Model | Purpose |
|-------|---------|
| `Transaction` | Input — one row from `transactions_rfn` |
| `MerchantExtractor` | Thin wrapper: merchant name + `is_person` flag |
| `MerchantMetaInput` | Input to the metadata agent |
| `MerchantMetaData` | Agent output — name, categories, city |
| `MetadataResult` | DB write model for `merchant_metadata_raw` |
| `SearchToolResult` | Web-search result passed between agents |

---

---

# Grocery Agentic Pipeline

The grocery agentic pipeline categorises each article in `groceries_rfn` using a
ChromaDB vector-store cache combined with a single LLM agent. No web search is needed —
grocery articles are categorised purely from their name and store location.

---

## Flow

```
groceries_rfn (enrichment_status='pending')
        │
        ▼
  GroceryManager.run()
        │
        ├─► vector-store cache hit?  ─── YES ──► use cached GroceryCategoryData
        │                                        skip LLM call
        │
        └─► NO ──► [grocery_agent] article_normalized + location → GroceryCategoryData
                         │
                         ▼
                   save to ChromaDB vector store (for future cache hits)
                         │
                         ▼
                   save to grocery_categorization_raw (SQLite)
                         │
                         ▼
                   mark groceries_rfn.enrichment_status = 'enriched'
```

---

## Entry Points

| File | Purpose |
|------|---------|
| `pipeline_agentic/grocery_manager.py` | `load_pending_groceries()` + `run_all_groceries()` |
| `pipeline_agentic/grocery_manager.py` | `GroceryManager.run()` — per-article orchestration |

**Running the full grocery enrichment:**
```python
import asyncio
from swiss_exp_tracker.pipeline_agentic.grocery_manager import (
    load_pending_groceries, run_all_groceries
)
from swiss_exp_tracker.pipeline_ingestion.stages.groceries.stage_04_use import (
    run_groceries_use
)

rows = load_pending_groceries()
asyncio.run(run_all_groceries(rows))
run_groceries_use()   # populates groceries_use after enrichment
```

---

## Agent

### grocery_agent — Categorisation Agent
**File:** `pipeline_agentic/agents_/agent_grocery.py`

Receives `article_normalized` and `location` as a JSON payload. Returns a
`GroceryCategoryData` with a two-level category assignment.

**Model:** `gpt-4o-mini`

**Output guardrail:** rejects results where `category_main` is not a valid
`GroceryCategoryMain` value, forcing the agent to retry (up to 3 attempts).

**Output:** `GroceryCategoryData`
```python
class GroceryCategoryData(BaseModel):
    article: str
    category_main: GroceryCategoryMain
    category_detail: GroceryCategoryDetail
```

---

## Categories

Defined as `StrEnum` in `pipeline_agentic/data_models/grocery.py`.

### GroceryCategoryMain
| Value | Typical contents |
|-------|-----------------|
| `Fresh Produce` | Fruit, vegetables, herbs |
| `Dairy & Eggs` | Milk, cheese, yoghurt, eggs |
| `Bakery & Bread` | Bread, croissants, pastries |
| `Meat & Fish` | Meat, poultry, fish, seafood |
| `Pasta & Grains` | Pasta, rice, flour, cereal |
| `Canned & Preserved` | Tinned goods, jars, sauces, condiments |
| `Frozen Foods` | Frozen meals, ice cream |
| `Snacks & Sweets` | Chips, chocolate, biscuits, candy |
| `Beverages` | Water, juice, coffee, tea, beer, soft drinks |
| `Ready Meals` | Pre-made hot or cold meals |
| `Personal & Household` | Cleaning, hygiene, household items |
| `Other` | Anything that doesn't fit above |

### GroceryCategoryDetail (selected)
Sub-categories refine `GroceryCategoryMain`. Full list in `data_models/grocery.py → GroceryCategoryDetail`. Key values:

- Fresh Produce → `Fruit | Vegetables | Herbs & Spices | Salad`
- Dairy & Eggs → `Milk | Cheese | Yoghurt | Butter | Eggs | Cream`
- Bakery & Bread → `Bread | Pastry | Cake`
- Meat & Fish → `Beef | Pork | Poultry | Fish | Seafood | Vegan Meat | Deli & Cold Cuts`
- Pasta & Grains → `Pasta | Rice | Flour | Cereal & Muesli | Legumes`
- Canned & Preserved → `Canned Goods | Sauces & Condiments | Oils & Vinegar | Jam & Spreads | Soup`
- Beverages → `Water | Juice | Coffee | Tea | Beer | Wine & Spirits | Soft Drinks | Energy Drinks`
- Snacks & Sweets → `Chocolate | Candy | Chips & Crisps | Nuts | Ice Cream | Biscuits & Cookies`

---

## Vector-Store Cache

**Implementation:** ChromaDB (`PersistentClient`) with cosine-similarity embeddings.
**Location:** `grocery_vector_store/`
**Class:** `GroceryStore` (`pipeline_agentic/grocery_store.py`)

| Operation | Threshold | Behaviour |
|-----------|-----------|-----------|
| `search(article_normalized)` | 0.85 | Returns `(GroceryCategoryData, similarity)` on hit, `None` on miss |
| `save(article_normalized, category)` | 0.93 | Skips upsert if a near-identical entry already exists |

The store is separate from `merchant_vector_store/` — grocery embeddings and merchant
embeddings live in different ChromaDB collections and directories.

---

## Database Tables — `transactions.db`

### `grocery_categorization_raw`
Full history — one row per categorisation run (cache hits and LLM calls alike).

| Column | Notes |
|--------|-------|
| `rfn_id` | FK → `groceries_rfn.id` |
| `article` | Original article name |
| `matched_article` | Normalised name that was looked up / stored |
| `cache_hit` | 1 = vector-store hit, 0 = LLM call |
| `similarity` | Cosine similarity score (cache hit only, else NULL) |
| `category_main` | `GroceryCategoryMain` value |
| `category_detail` | `GroceryCategoryDetail` value |

### `grocery_categorization_rfn` (VIEW)
Latest categorisation result per `rfn_id`. Used by `stage_04_use.py` when building
`groceries_use`. Defined in `GroceryResult.__init__()`.

---

## Concurrency

`run_all_groceries()` uses:
- `asyncio.Semaphore(10)` — max 10 articles in flight simultaneously
- Per-article `asyncio.Lock` (keyed on `article_normalized.lower()`) — prevents two
  rows with the same normalised article name from calling the LLM concurrently,
  ensuring the first result is cached before the second starts

---

## Data Models

All models are Pydantic `BaseModel` subclasses in `pipeline_agentic/data_models/grocery.py`.

| Model | Purpose |
|-------|---------|
| `GroceryRow` | Input — one row from `groceries_rfn` |
| `GroceryCategoryData` | Agent output — article, category_main, category_detail |
| `GroceryCategoryResult` | DB write model for `grocery_categorization_raw` |
| `GroceryCategoryMain` | `StrEnum` — 12 top-level categories |
| `GroceryCategoryDetail` | `StrEnum` — ~40 sub-categories |
