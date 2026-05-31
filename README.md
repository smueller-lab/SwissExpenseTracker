# 🇨🇭 Swiss Expense Tracker 📈

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Dash](https://img.shields.io/badge/Dashboard-Plotly_Dash-informational)
![AI](https://img.shields.io/badge/AI-OpenAI_Agents-blueviolet)
![Status](https://img.shields.io/badge/status-active-brightgreen)

👤 **Author**: _Sebastian Müller – Data Scientist & AI Engineer_

💡 **Status:** This project is under active development. Features may change, and some components may not be fully stable yet. Feedback is welcome!

A personal finance pipeline and dashboard for Swiss banking data. Drop your bank exports in a folder, run one command, and get a fully enriched, interactive dashboard — merchant categories, city data, grocery breakdowns, and investment tracking, all powered by AI agents.

> 🏦 Built for ZKB, Viseca, Revolut, Swissquote, and Migros.

---

## 📋 Table of Contents

- [✨ What it does](#-what-it-does)
- [📊 Dashboard](#-dashboard)
- [🤖 The Agentic Pipeline](#-the-agentic-pipeline)
- [🏷️ Transaction Categories](#-transaction-categories)
- [🚀 Getting Started](#-getting-started)
  - [📁 Download your bank data](#-download-your-bank-data)
  - [🔑 Configure API keys](#-configure-api-keys)
  - [📦 Install](#-install)
  - [▶️ Run the pipeline](#-run-the-pipeline)
  - [🖥️ Launch the dashboard](#-launch-the-dashboard)
- [📚 Dev Docs](#-dev-docs)
- [🛠️ Tech Stack](#-tech-stack)

---

## ✨ What it does

- 📥 **Ingests** bank CSV/XLS exports from multiple Swiss sources with full deduplication
- 🤖 **Enriches** every transaction using an AI agent pipeline — web search + LLM extracts merchant name, category, and city
- ⚡ **Caches** results in a local ChromaDB vector store so each merchant is only looked up once
- 🛒 **Categorises** grocery articles at item level (from Migros receipts) using a second agent pipeline
- 📈 **Tracks** Swissquote investment portfolio snapshots over time
- 🎨 **Visualises** everything in a dark-themed interactive Plotly Dash dashboard

---

## 📊 Dashboard

> 📸 Screenshots coming soon.

### Pages

| Page | What you see |
|---|---|
| 🏠 **Home** | Balance progression, net balance per month, top spending category, expense distribution |
| 🛒 **Groceries** | Store breakdown (Migros, Coop, Lidl, Aldi, ...), spend distribution |
| **M Cumulus Analytics** | Item-level grocery analysis: categories, health score, top articles |
| 🍽️ **Dining & Bars** | Restaurant & grocery spend by frequency, per-visit box plots |
| 🏖️ **Vacation** | Annual travel spend by type (flights, hotels, car rental) |
| 🚄 **Transport** | Yearly transport costs by subcategory, monthly heatmap, car expenses |
| ⛳ **Sport** | Sport spending by activity type over time |
| 🛍️ **Retail** | Retail breakdown by subcategory, spend donut, top purchases |
| 🔍 **Smart Table** | Fully filterable transaction browser |
| 📈 **Investing** | Portfolio value vs. invested, P&L, per-position progression |

---

## 🤖 The Agentic Pipeline

The heart of this project. Bank transaction texts are often cryptic or abbreviated — a web search finds the actual merchant, and an LLM structures it into clean data.

```
transactions_rfn (pending enrichment)
        │
        ├─► 👤 known person (TWINT)?  ──► labelled as FRIEND, no API call
        │
        ├─► ⚡ ChromaDB vector cache hit?  ──► reuse metadata, no web search
        │
        └─► 🔍 Summary Agent  (gpt-4.1-mini + web search)
                  │  merchant summary text
                  ▼
            🏷️ Metadata Agent  (gpt-4.1-mini)
                  │  name · category · city
                  ▼
            💾 save to ChromaDB  ──► future transactions skip web search
                  │
                  ▼
            ✅ transactions_use  (dashboard-ready)
```

### 🌐 Smart web search

Free tiers are exhausted before falling back to pay-per-use. API credit consumption is tracked monthly per provider.

| Priority | Provider | Free quota |
|---|---|---|
| 1 | Tavily | 1 000 req/month |
| 2 | Brave Search | 1 000 req/month |
| 3 | Scrape.do | 1 000 req/month |
| 4 | Exa | 500 credits/month |
| 5–6 | Tavily / Exa (pay-per-use) | unlimited |

⚙️ **Concurrency** — 5 transactions run in parallel. Unique merchants are prioritised first so cache entries are warm before repeated merchants are processed.

🛒 **Grocery enrichment** runs a separate, lighter pipeline: no web search needed — a single LLM agent categorises articles from their name and store location using a grocery-specific ChromaDB cache.

---

## 🏷️ Transaction Categories

Two-level hierarchy assigned by the metadata agent:

**Main categories:**
`Sport` · `Entertainment` · `Telecommunication` · `Restaurant` · `Healthcare` · `Government` · `Retail` · `Groceries` · `Salary` · `Housing` · `Car` · `Transport` · `Travel` · `Insurance` · `Education` · `Payment Services` · `Investing` · `Postal Services` · `Friend`

Sub-categories refine each main category (e.g. Sport → Tennis / Padel / Fitness / Cycling; Restaurant → Dining / Fast Food / Cafe / Bar).

---

## 🚀 Getting Started

### 📁 Download your bank data

#### 🏦 ZKB (Zürcher Kantonalbank)

1. Log in to ZKB eBanking.
2. **Account & Payments → Private account**
3. Click **More options**, select your time frame.
4. Scroll to the bottom, click **Show all**.
5. Top right: **CSV → with Details** — the `with Details` option is required.

Place the file in `lnd/zkb/`.

#### 💳 Viseca (credit card)

1. Log in to ZKB eBanking.
2. **Cards → Credit cards → Overview VisecaOne → Next**
3. Click **Bills → Download .csv**, select your time frame.

Alternatively, log in directly at visecaone.com.

Place the file in `lnd/viseca/`.

#### 🔄 Revolut

_How to export: coming soon._

Place the file in `lnd/revolut/`.

#### 📈 Swissquote (investment positions)

_How to export: coming soon._

Place the file in `lnd/swissquote/`.

#### 🛒 Migros (grocery receipts via M Cumulus)

_How to export: coming soon._

Place the file in `lnd/migros_grocery/`.

---

### 🔑 Configure API keys

Create a `.env` file in the project root:

```env
OPENAI_API_KEY=sk-...

# Web search providers (at least one required)
TAVILY_API_KEY=...
BRAVE_API_KEY=...
SCRAPE_DO_API_KEY=...
EXA_API_KEY=...
```

You only need keys for the providers you want to use — the pipeline tries them in order and skips any that are unconfigured. **Tavily** has a generous free tier and is the recommended starting point.

For OpenAI, `gpt-4.1-mini` is used by default — inexpensive and accurate enough for categorisation. Check [OpenAI pricing](https://platform.openai.com/docs/pricing) before running.

---

### 📦 Install

```bash
git clone https://github.com/smueller-lab/SwissExpenseTracker.git
cd SwissExpenseTracker
python3 -m venv venv && source venv/bin/activate
pip install poetry && poetry install
```

---

### ▶️ Run the pipeline

```bash
poetry run python src/swiss_exp_tracker/pipeline.py
```

This runs all stages in order:
1. 📥 **Ingestion** — detects new files, validates rows, normalises merchants
2. 🤖 **Agentic enrichment** — AI agent looks up merchant metadata via web search
3. 🛒 **Grocery enrichment** — AI agent categorises individual Migros articles
4. 🔗 **Final join** — produces the dashboard-ready `transactions_use` table

♻️ Re-running is safe — already-processed files and transactions are skipped.

---

### 🖥️ Launch the dashboard

```bash
poetry run python src/swiss_exp_tracker/app/app.py
```

Open [http://localhost:8050](http://localhost:8050) in your browser.

---

## 📚 Dev Docs

Detailed technical documentation lives in `.dev-docs/`:

| Doc | Contents |
|---|---|
| [`01-agentic-pipeline.md`](.dev-docs/01-agentic-pipeline.md) | Agent architecture, ChromaDB cache, web search chain, data models |
| [`02-ingestion-pipeline.md`](.dev-docs/02-ingestion-pipeline.md) | All ingestion stages, supported sources, DB schema |
| [`03-dashboard.md`](.dev-docs/03-dashboard.md) | Every dashboard page, KPI cards, chart specs |
| [`04-pipeline-dash.md`](.dev-docs/04-pipeline-dash.md) | Pre-aggregation pipeline that feeds the dashboard |

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Pipeline & data models | Python 3.12, Pydantic v2, SQLite |
| AI agents | OpenAI Agents SDK (`gpt-4.1-mini`) |
| Merchant cache | ChromaDB (cosine-similarity vector store) |
| Web search | Tavily, Brave Search, Scrape.do, Exa |
| Dashboard | Plotly Dash, Plotly |
| Dependency management | Poetry |
