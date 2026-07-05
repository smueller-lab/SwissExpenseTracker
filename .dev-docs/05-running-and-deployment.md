# 05 — Running & Deployment

How to run the full data pipeline and the dashboard, both locally (Poetry) and in
containers (Docker Compose). Covers configuration, the orchestration entry point,
and how the version shown in the UI is resolved.

---

## Two things you can run

| What | Entry point | Command |
|------|-------------|---------|
| **Full data pipeline** | `swiss_exp_tracker/pipeline.py` (`main()`) | `python -m swiss_exp_tracker.pipeline` |
| **Dashboard** | `swiss_exp_tracker/app/app.py` (`server` / `app`) | `python -m swiss_exp_tracker.app.app` (dev) or gunicorn (prod) |

The pipeline writes the SQLite databases; the dashboard reads them. Run the
pipeline first, then launch the dashboard.

---

## Top-level pipeline orchestration

**File:** `src/swiss_exp_tracker/pipeline.py → main()`

Runs the whole data flow in five sequential stages. Each underlying stage is
idempotent, so re-running the whole pipeline only processes new data.

```
python -m swiss_exp_tracker.pipeline
        │
        ├─ Stage 1  run_ingestion()            landing → raw → refined → postprocess
        │                                       (+ positions + groceries sub-pipelines)
        ├─ Stage 2  run_all_transactions(...)   agentic web-search + metadata enrichment
        │              (skipped if load_pending_transactions() returns nothing)
        ├─ Stage 3  run_post_clean()            manual corrections → merchant_metadata_rfn
        ├─ Stage 4  run_transactions_use()      final analysis join → transactions_use
        └─ Stage 5  run_dashboard_pipeline()    pre-aggregated dash_* tables
```

- Stage 1 detail → [`02-ingestion-pipeline.md`](02-ingestion-pipeline.md)
- Stage 2/3 detail → [`01-agentic-pipeline.md`](01-agentic-pipeline.md)
- Stage 5 detail → [`04-pipeline-dash.md`](04-pipeline-dash.md)

Stage 2 wraps the async enrichment in a single `asyncio.run(...)`. Logging is
configured at module import (`logging.basicConfig`, INFO level, `HH:MM:SS` format).

Note: the dashboard also calls `run_dashboard_pipeline()` itself at import time
(`app/app.py`), so the `dash_*` tables are always rebuilt from the latest
`transactions_use` when the app boots — even if you only ran the pipeline earlier.

---

## Configuration

### `DATA_DIR` (required)

The app must know where your bank exports live. `user_config.py` reads the
`DATA_DIR` environment variable (via `python-dotenv`, so a `.env` file works) and
exposes it as `DIR_DATA`. It raises `RuntimeError` at import if unset.

```
DATA_DIR  →  DIR_DATA  →  LANDING_ZONE_DIR = DIR_DATA / "lnd"
```

The landing zone (`<DATA_DIR>/lnd/`) holds one sub-folder per source (e.g.
`lnd/migros_grocery/`, `lnd/swissquote/`). See `pipeline_ingestion/config.py`.

### API keys

The agentic enrichment pipeline needs an `OPENAI_API_KEY`. Web-search providers
(`TAVILY_API_KEY`, `BRAVE_SEARCH_API_KEY`, `EXA_API_KEY`, `SCRAPE_DO_API_KEY`) are
optional and used via the fallback chain in
[`01-agentic-pipeline.md`](01-agentic-pipeline.md). `VISECA_CARD` / `VISECA_SESS`
are only needed when fetching Viseca data.

Copy `.env.example` → `.env` and fill in your own keys. `.env` is gitignored and
is **never** copied into the Docker image — keys stay on your machine.

### Databases & vector stores (on-disk paths)

The app resolves these relative to the current working directory (so always run
from the project root, or `WORKDIR /app` in Docker):

| Path | Contents |
|------|----------|
| `database/transactions.db` | all transaction, grocery, merchant-metadata, and `dash_*` tables |
| `database/positions.db` | Swissquote position snapshots |
| `merchant_vector_store/` | ChromaDB merchant cache |
| `grocery_vector_store/` | ChromaDB grocery cache |

---

## Running locally (Poetry)

```bash
poetry install
cp .env.example .env          # then fill in DATA_DIR + OPENAI_API_KEY

poetry run python -m swiss_exp_tracker.pipeline      # build the databases
poetry run python -m swiss_exp_tracker.app.app       # dev server on :8050
```

The dev server uses Dash's built-in Flask server with `debug=True`
(`app.run(debug=True)` in `app/app.py`). Use it for development only.

---

## Running with Docker

One image (`Dockerfile`) serves both the pipeline and the dashboard;
`docker-compose.yml` defines two services off that single image.

### Image (`Dockerfile`)

- Base `python:3.12-slim`, `WORKDIR /app`, `PYTHONPATH=/app/src` (package importable
  without an editable install).
- Installs deps with Poetry (`virtualenvs.create false`, `--no-root --only main`)
  in a cached layer, then installs the root package so `importlib.metadata` can
  read the version (see below).
- `gunicorn` is installed in the image only — it is the production WSGI server and
  is deliberately **not** in `pyproject.toml` (local dev uses the Dash dev server).
- `CMD` runs gunicorn against `swiss_exp_tracker.app.app:server` (the Flask server
  exposed by `server = app.server`), bound to `0.0.0.0:8050`, `--workers 2`,
  `--preload` (load data once in the master, shared with workers via copy-on-write),
  `--timeout 180` (startup rebuilds the `dash_*` tables).

### Services (`docker-compose.yml`)

| Service | Profile | Purpose | Command |
|---------|---------|---------|---------|
| `dashboard` | default | serves the UI on `:8050` (gunicorn) | image `CMD` |
| `pipeline` | `tools` | runs ingestion + enrichment on demand | `python -m swiss_exp_tracker.pipeline` |

Both mount the same volumes so the pipeline's output is visible to the dashboard:

```
./bank_data                → /data       (DATA_DIR is overridden to /data)
./database                 → /app/database
./merchant_vector_store    → /app/merchant_vector_store
./grocery_vector_store     → /app/grocery_vector_store
```

`DATA_DIR` is set to `/data` in the compose `environment:` block, overriding
whatever is in `.env` so it points inside the container. API keys come from
`.env` via `env_file:`.

### Typical flow

```bash
# 1. Put bank exports under ./bank_data/  (must contain an  lnd/  folder)
# 2. Copy .env.example → .env and fill in your OWN keys
docker compose run --rm pipeline      # build + enrich the database (profile: tools)
docker compose up dashboard           # serve → http://localhost:8050
```

The `pipeline` service is under the `tools` profile, so `docker compose up` starts
only the dashboard; the pipeline is run explicitly with `docker compose run`.

---

## App version in the UI

The version shown in the dashboard sidebar comes from the installed package
metadata, not a hardcoded string.

- `app/app.py → _app_version()` calls `importlib.metadata.version("swiss_exp_tracker")`
  and returns `""` on `PackageNotFoundError`.
- It resolves to the `version` in `[tool.poetry]` of `pyproject.toml` (currently
  `0.1.0`) **only when the package is installed**. This is why the Dockerfile runs a
  final `poetry install --only main` for the root package — without it the metadata
  is missing and the version renders empty.
- `_app_version()` is passed into `create_app_layout(app_version=...)`.

The sidebar also shows a "data through" label from `_data_through_label()` — the
most recent transaction month (`pdf_Master["date"].max()` formatted `"%b %Y"`).
