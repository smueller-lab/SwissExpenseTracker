# Swiss Expense Tracker — single image used for both the pipeline and the dashboard.
FROM python:3.12-slim

# - PYTHONUNBUFFERED: stream logs straight to the container output
# - PYTHONPATH: make the `swiss_exp_tracker` package importable without a build/install step
# - WORKDIR /app: the app resolves ./database and ./*_vector_store relative to the cwd
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    POETRY_VERSION=2.1.4 \
    GUNICORN_VERSION=23.0.0

WORKDIR /app

# gunicorn is the production WSGI server for the dashboard; it is only used inside
# the container (local dev still uses the Dash dev server), so it is installed here
# rather than added to pyproject. Pinned so an image rebuild can't silently change
# the WSGI server version (everything else is pinned via poetry.lock).
RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}" "gunicorn==${GUNICORN_VERSION}"

# Install dependencies first (cached unless the lock file changes). --no-root skips
# building the package itself here so this layer stays cached when only source changes.
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root --only main

# Application source. README.md is required by poetry to install the root package.
COPY src ./src
COPY README.md ./

# Install the root package so its metadata is available. importlib.metadata.version()
# powers the version shown in the dashboard sidebar; without this it reads as empty.
# Dependencies are already installed above, so this only adds the root package.
RUN poetry install --no-interaction --no-ansi --only main

EXPOSE 8050

# Verify the dashboard actually answers HTTP, not just that the process is alive —
# a wedged gunicorn won't exit, so `restart: unless-stopped` alone can't catch it.
# start-period is generous because startup rebuilds the dash_* tables before serving.
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8050/').read()" || exit 1

# Serve the Flask server exposed by app.py (`server = app.server`).
# --preload loads data once in the master before forking workers (shared via COW);
# --timeout is generous because the dashboard loads + rebuilds tables at startup.
CMD ["gunicorn", "swiss_exp_tracker.app.app:server", \
     "--bind", "0.0.0.0:8050", \
     "--workers", "2", \
     "--preload", \
     "--timeout", "180", \
     "--access-logfile", "-"]
