# Swiss Expense Tracker — single image used for both the pipeline and the dashboard.
FROM python:3.12-slim

# - PYTHONUNBUFFERED: stream logs straight to the container output
# - PYTHONPATH: make the `swiss_exp_tracker` package importable without a build/install step
# - WORKDIR /app: the app resolves ./database and ./*_vector_store relative to the cwd
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    POETRY_VERSION=1.8.3

WORKDIR /app

# gunicorn is the production WSGI server for the dashboard; it is only used inside
# the container (local dev still uses the Dash dev server), so it is installed here
# rather than added to pyproject.
RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}" gunicorn

# Install dependencies first (cached unless the lock file changes). --no-root skips
# building the package itself — PYTHONPATH makes the source importable instead.
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root --only main

# Application source
COPY src ./src

EXPOSE 8050

# Serve the Flask server exposed by app.py (`server = app.server`).
# --preload loads data once in the master before forking workers (shared via COW);
# --timeout is generous because the dashboard loads + rebuilds tables at startup.
CMD ["gunicorn", "swiss_exp_tracker.app.app:server", \
     "--bind", "0.0.0.0:8050", \
     "--workers", "2", \
     "--preload", \
     "--timeout", "180", \
     "--access-logfile", "-"]
