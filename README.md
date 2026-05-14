# 🇨🇭 Swiss Expense Tracker 📈

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Poetry](https://img.shields.io/badge/Poetry-managed-yellow)
![Project Status](https://img.shields.io/badge/status-in--development-yellow)

👤 **Author**: _Sebastian Müller – Data Scientist_

💡 **Status:** This project is under active development. Features may change, and some components may not be fully stable yet. Feedbacks are welcome!

## Table of Contents

- [ℹ️ Description](#ℹ%EF%B8%8F-description)
- [📝 Overview](#-overview)
  * [📁 Directory tree for box](#-directory-tree-for-box)
- [🚀 Getting Started](#-getting-started)
  * [📁 Download your transactional data](#-download-your-transactional-data)
    + [🏦 Zuercher Kantonalbank](#-zuercher-kantonalbank)
    + [💳 Viseca One](#-viseca-one)
  * [✅ Prerequisites](#-prerequisites)
  * [📦 Installation](#-installation)
  * [💡 Usage of the OpenAI API](#-usage-of-the-openai-api)
  * [▶ Run data pipeline](#-run-data-pipeline)
- [📊 Visualisation &amp; Reporting](#-visualisation--reporting)
- [🗠 Analysis](#-analysis)
- [� Pipeline](#-pipeline)
  * [Stage 1 — Ingestion Pipeline](#stage-1--ingestion-pipeline)
  * [Stage 2 — Agentic Enrichment Pipeline](#stage-2--agentic-enrichment-pipeline)
  * [Stage 3 — Final Join](#stage-3--final-join)
  * [Categories](#categories)
- [🀽� Some comments about the code base](#-some-comments-about-the-code-base)

## ℹ️ Description

Expense tracking tool by labeling all transactions from different sources with the OpenAI API call.

Transactional data from ZKB, Viseca and Revolut.

This project gives a baseline how to label and clean your transactional data. If you are part of a different bank or are even coming from a different country your data might look different and therefore you also need to adapt my code so it fits the structure of your data.

---

## 📝 Overview

**ExpenseTracker** is a personal finance pipeline designed to ingest, clean, enrich, and analyze expenses from typical swiss banking sources as **ZKB Debit Card**, **Viseca Credit Card** and **Revolut**.

**Viseca** already offers own transaction categorization but it's not very accurate. The goal of this project is to generate more accurate and detailed transaction labels using the OpenAI API. This enables deeper insights during financial analysis.

The project uses clear data zone architecture:

- **lnd**: That's where we copy all the raw data files, which we download in beforehand from our bank. (Example: zkb_20200112_1.csv). When there are multiple data files downloaded from the same day we add an incremental number.
- **raw**: In raw, the files are seperated by source and are transformed to **pkl** files. (See xx_ing_xx.py)
- **rfn**: Refined is the biggest stage where the files are saved with the new labels from OpenAI. (labelAI, labelAI_cleaned). As OpenAI is not labeling every transaction correct as the data may contain smaller shops which are not very known and the label can also not be extracted out of the shop's name, we need to perform some postprocessing to correct and fill some missing labels.
- **use**: Here we have the finished labeled, cleaned and transformed files which are then ready to be analysed.

### 📁 Directory tree for box

```bash
├── lnd
│   ├── DebitCard
│   ├── Revolut
│   └── Viseca
├── raw
│   ├── DebitCard
│   └── Viseca
├── rfn
│   ├── DebitCard
│   │   ├── labelAI
│   │   ├── labelAI_cleaned
│   │   └── Master
│   └── Viseca
│       ├── labelAI
│       └── labelAI_cleaned
└── use
    ├── Bank_ZKB
    ├── DebitCard
    └── Viseca
```

The pipeline is built for **repeatable, maintainable** processing — with smart caching to avoid reprocessing and customizable rules for categorization and enrichment.

---

## 🚀 Getting Started

### 📁 Download your transactional data

A short guide is provided how to get the data files from the banks I'm using.

#### 🏦 Zuercher Kantonalbank

1. Log-in to your eBanking of ZKB.
2. Go to **Account & Payments** --> **Private account**.
3. On top of your transactions click **More options** and select the desired time frame.
4. Go to the bottom of the page and click **Show all** to download all transactions of your sepecified time frame.
5. In the top right corner click **CSV** --> **with Details** and the transactions are downloaded to your computer. (Don't forget the **with Details** part. Without that you will get uncomplete data for  the eBanking transactions).

#### 💳 Viseca One

There are two different ways how to download the Viseca data. The first unfortunately doesn't give us the Valuta Date which is important to properly merge both data sources.

##### 1. Download the data via the Viseca-exporter repo

At the repo: [viseca-exported](https://github.com/anothertobi/viseca-exporter) you can find a good guideline how to download the Viseca data.
This approach works well but is unfornately missing the Valuta Date because it doesn't appear on the screen when accesing the Viseca transactions.

Thanks a lot to the contributors who worked on that repo!

##### 2. Download the data directly over the interface of the Viseca website [NEW]

During the development of the pipeline I found out that Viseca has changed the interface of their website.

1. Log-in to your eBanking of ZKB.
2. Go to **Cards** --> **Credit cards**
3. On the right side you already see the Viseca logo. Click **Overview VisecaOne** --> **Next**
4. Click on **Bills** and here at the top right corner you see a **Download .csv** button. Click it and select your desired time frame.

If you are at a different bank you can also Log-in directly to VisecaOne.

#### 🇷 Revolut

_Coming soon..._

### ✅ Prerequisites

- Python 3.12 or later
- [Poetry](https://python-poetry.org/docs/#installation) for dependency management
- CSV files downloaded from your bank or credit card provider
- OpenAI account to use the API

### 📦 Installation

1. **Clone the repository**

```bash
git clone https://github.com/smueller-lab/SwissExpenseTracker.git
cd expensetracker
```

2. **Create virtualenv and install poetry dependencies**

```bashs
python3 -m venv venv
source venv/bin/activate
poetry install
```

### 💡 Usage of the OpenAI API

Using the OpenAI API comes with some cost. OpenAI offers good models with low cost.
You can of course also use a different API.

1. First you need to create an OpenAI account.
2. If you're logged in go to: [OpenAI_API_keys](https://platform.openai.com/api-keys) and create your own key.
3. Save your key into an `.env` file which you put into the root directory of the cloned project.
4. Very important: check the pricing of the different models: [OpenAI_Pricing](https://platform.openai.com/docs/pricing). I have used gpt-4.1-mini which is not very expensive and gives good result. I didn't notice a major improvement when trying out their more recent and best models.
5. Use my code in `OpenAI.py` or use the Quickstart from OpenAI: [OpenAI_Quickstart](https://platform.openai.com/docs/quickstart). You might want to change the prompt to your needs and give examples to better label your transactional data.

## 📊 Visualisation & Reporting

_Coming soon..._

## 🗠 Analysis

_Coming soon..._

---

## 🔁 Pipeline

Drop your bank CSV exports into the landing zone and run one command — the pipeline takes care of the rest:

```bash
poetry run python src/swiss_exp_tracker/pipeline.py
```

```
 Bank CSV exports  (ZKB · Viseca · Revolut)
         │
         ▼
 ┌───────────────────────────────────────────────────────┐
 │  1 · INGESTION PIPELINE                               │
 │                                                       │
 │  lnd → raw → refined → postprocess                   │
 │                                                       │
 │  • Detects new files by MD5 hash (no re-imports)     │
 │  • Validates rows with source-specific Pydantic models│
 │  • Normalises merchant names & flags P2P transfers    │
 │  • Removes credit-card settlement duplicates          │
 └───────────────────────────────────────────────────────┘
         │
         │  transactions_rfn  (pending enrichment)
         ▼
 ┌───────────────────────────────────────────────────────┐
 │  2 · AGENTIC ENRICHMENT PIPELINE                      │
 │                                                       │
 │  For each pending transaction:                        │
 │                                                       │
 │  ① Vector-store cache hit?  ──yes──► reuse metadata  │
 │            │ no                                       │
 │            ▼                                          │
 │  ② Summary Agent  searches the web for merchant info │
 │            │                                          │
 │  ③ Metadata Agent  extracts category + city          │
 │            │                                          │
 │  ④ Cache result in ChromaDB for future transactions  │
 └───────────────────────────────────────────────────────┘
         │
         │  merchant_metadata_rfn  (categories + city)
         ▼
 ┌───────────────────────────────────────────────────────┐
 │  3 · FINAL JOIN                                       │
 │                                                       │
 │  transactions_rfn  ✕  merchant_metadata_rfn           │
 │          └──────────────────► transactions_use        │
 └───────────────────────────────────────────────────────┘
         │
         ▼
  📊 Dashboard  (analysis-ready)
```

---

### Stage 1 — Ingestion

Raw CSV files are loaded and passed through four sequential sub-stages, each writing to its own SQLite table:

| # | Sub-stage | Table | What happens |
|---|-----------|-------|--------------|
| 1 | **Landing** | `transactions_lnd` | Files are scanned; only new ones (by MD5 hash) are ingested |
| 2 | **Raw** | `transactions_raw` | Each row is validated against a source-specific Pydantic model |
| 3 | **Refined** | `transactions_rfn` | Fields are unified, merchant names normalised, P2P transfers flagged |
| 4 | **Postprocess** | `transactions_rfn` | Credit-card settlement duplicates removed; Viseca fee labels corrected |

---

### Stage 2 — Agentic Enrichment

Bank transaction texts alone are often too cryptic to categorise reliably. This stage uses an AI-agent pipeline to look up what each merchant actually is.

**Why agents + web search?** Merchant names from bank statements are frequently abbreviated, mangled, or entirely unknown to a static lookup table. A web search finds the actual business — its type, city, and category — which an LLM then structures into the data model. Results are cached in a local ChromaDB vector store so each merchant is only looked up once, no matter how many transactions it appears in.

**Web search — free-tier-first fallback chain:**

| Priority | Provider | Free quota |
|----------|----------|------------|
| 1 | Tavily | 1 000 requests / month |
| 2 | Brave Search | 1 000 requests / month |
| 3 | Scrape.do | 1 000 requests / month |
| 4 | Exa | 500 credits / month |
| 5–6 | Exa / Tavily pay-as-you-go | unlimited |

Monthly credit usage is tracked in the `api_usage` table so the pipeline never exceeds configured limits.

**Concurrency:** 5 transactions are enriched in parallel to keep web-search latency manageable. Unique merchants are prioritised first so that by the time a repeated merchant appears, its cache entry is already warm.

---

### Stage 3 — Final Join

`transactions_rfn` (transaction fields) is joined with `merchant_metadata_rfn` (categories + city) to produce `transactions_use` — the single table consumed by the dashboard.

---

### Categories

Transactions are labelled with a two-level category hierarchy:

**Main categories:** Sport · Entertainment · Telecommunication · Restaurant · Healthcare · Government · Retail · Groceries · Salary · Housing · Car · Transport · Travel · Insurance · Education · Payment Services · Investing · Postal Services · Friend
