# Contributor dev environment for Swiss Expense Tracker.
#
# This is NOT the production image (see ../Dockerfile for that). The prod image
# installs `--only main` and runs under gunicorn; this one gives contributors the
# full dependency set INCLUDING the dev group (ruff, black, mypy, pytest, kaleido)
# so they can lint, type-check, test, and run the Dash dev server.
#
# Base is Microsoft's dev container image: it pins the same Python 3.12 as prod
# and ships a non-root `vscode` user, git, and shell tooling ready to go.
FROM mcr.microsoft.com/devcontainers/python:3.12

# Pin Poetry to a 2.1.x release to match this repo's poetry.lock (lock-version
# 2.1 — written by Poetry 2.x). Poetry 1.8 reads a 2.1 lock only in a degraded
# mode that skips the root-package install, so the project would not be importable.
#
# The virtualenv lives in-project at ./.venv (IN_PROJECT=true). devcontainer.json
# mounts a named volume over that path so the venv is container-local (the host's
# own .venv, visible through the workspace bind mount, is shadowed) and persists
# across rebuilds. Dependencies are therefore installed at postCreate time into
# that volume, not baked into this image — baking them here would just be shadowed.
ENV POETRY_VERSION=2.1.4 \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN pipx install "poetry==${POETRY_VERSION}"
