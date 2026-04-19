from __future__ import annotations

import asyncio
import os

from datetime import date
from datetime import datetime

import requests

from dotenv import load_dotenv
from pydantic import BaseModel

from swiss_exp_tracker.pipeline_ingestion.db import get_connection


load_dotenv(override=True)


class ApiKeys(BaseModel):
    openai_api_key: str | None = None
    serpapi_api_key: str | None = None
    tavily_api_key: str | None = None
    bright_data_api_key: str | None = None


def get_settings() -> ApiKeys:
    """Load API keys and other settings from environment variables."""
    return ApiKeys(
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        serpapi_api_key=os.getenv("SERPAPI_API_KEY"),
        tavily_api_key=os.getenv("TAVILY_API_KEY"),
        bright_data_api_key=os.getenv("BRIGHT_DATA_API_KEY"),
    )


API_KEYS = get_settings()


# ── Bright Data ──────────────────────────────────────────────────────────────
BRIGHT_DATA_FREE_CREDIT_LIMIT: int = 5000

# ── Tavily ───────────────────────────────────────────────────────────────────
TAVILY_FREE_CREDIT_LIMIT: int = 1000

# ── SerpAPI ───────────────────────────────────────────────────────────────────────────
SERPAPI_FREE_CREDIT_LIMIT: int = 250
_serpapi_quota_exceeded = False


def is_serpapi_quota_exceeded() -> bool:
    return _serpapi_quota_exceeded


# ── API usage persistence (SQLite) ───────────────────────────────────────────


def _current_period() -> str:
    """Shared monthly period key (YYYY-MM) used for all provider credit buckets."""
    return date.today().strftime("%Y-%m")


def _ensure_api_usage_table() -> None:
    """Create the api_usage table if it doesn't exist yet."""

    with get_connection() as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS api_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                period TEXT NOT NULL,
                used INTEGER NOT NULL DEFAULT 0,
                credit_limit INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                UNIQUE(provider, period)
            )
            """
        )


def _load_api_usage(provider: str, period: str) -> int:
    """Return the stored *used* count for *provider* in *period*, or 0."""

    _ensure_api_usage_table()
    with get_connection() as db:
        row = db.execute(
            "SELECT used FROM api_usage WHERE provider = ? AND period = ?",
            (provider, period),
        ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _save_api_usage(provider: str, period: str, used: int, credit_limit: int) -> None:
    """Upsert the *used* count for *provider* / *period* into the DB."""

    _ensure_api_usage_table()
    try:
        with get_connection() as db:
            db.execute(
                """
                INSERT INTO api_usage (provider, period, used, credit_limit, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(provider, period) DO UPDATE SET
                    used = excluded.used,
                    credit_limit = excluded.credit_limit,
                    updated_at = excluded.updated_at
                """,
                (
                    provider,
                    period,
                    max(0, int(used)),
                    credit_limit,
                    datetime.now().astimezone().isoformat(),
                ),
            )
    except Exception:
        return None


def load_serpapi_usage() -> int:
    """Load SerpAPI usage from the DB for the current monthly period."""
    return _load_api_usage("serpapi", _current_period())


def save_serpapi_usage(used: int) -> None:
    """Persist SerpAPI usage for the current monthly period."""
    _save_api_usage("serpapi", _current_period(), used, SERPAPI_FREE_CREDIT_LIMIT)


def load_brightdata_usage() -> int:
    """Load Bright Data free-credit usage from the DB for the current monthly period."""
    return _load_api_usage("bright_data", _current_period())


def save_brightdata_usage(used: int) -> None:
    """Persist Bright Data free-credit usage for the current monthly period."""
    _save_api_usage(
        "bright_data", _current_period(), used, BRIGHT_DATA_FREE_CREDIT_LIMIT
    )


def brightdata_web_search(query: str) -> str:
    """Search via the Bright Data MCP server and return normalised results.

    Uses ``bright_data_mcp()`` from the MCP server registry.
    Requires BRIGHT_DATA_API_KEY to be set in .env.
    """
    if not API_KEYS.bright_data_api_key:
        return "BRIGHTDATA_UNAVAILABLE: BRIGHT_DATA_API_KEY is missing."

    async def _search() -> str:
        from swiss_exp_tracker.pipeline_agentic.mcp_servers import bright_data_mcp

        server = bright_data_mcp()
        try:
            async with server:
                result = await server.call_tool("search_engine", {"query": query})
        except BaseExceptionGroup as eg:
            causes = "; ".join(str(e) for e in eg.exceptions)
            return f"BRIGHTDATA_UNAVAILABLE: request failed ({causes})."
        except Exception as exc:
            return f"BRIGHTDATA_UNAVAILABLE: request failed ({exc})."

        if not result.content:
            return "BRIGHTDATA_UNAVAILABLE: no results."

        lines: list[str] = []
        for item in result.content:
            text = getattr(item, "text", None)
            if text:
                stripped = str(text).strip()
                lower = stripped.lower()
                credit_exhausted_phrases = (
                    "credit limit exceeded",
                    "credits exceeded",
                    "out of credits",
                    "quota exceeded",
                    "monthly quota",
                    "rate limit exceeded",
                )
                if any(phrase in lower for phrase in credit_exhausted_phrases):
                    return "BRIGHTDATA_CREDITS_EXCEEDED"
                if stripped:
                    lines.append(stripped)

        if not lines:
            return "BRIGHTDATA_UNAVAILABLE: no parseable results."

        return "BRIGHTDATA_RESULTS\n" + "\n".join(lines)

    return asyncio.run(_search())


# ── Tavily credit persistence (DB-backed) ────────────────────────────────────


def load_tavily_usage() -> int:
    """Load Tavily free-credit usage from the DB for the active period."""
    return _load_api_usage("tavily", _current_period())


def save_tavily_usage(used: int) -> None:
    """Persist Tavily free-credit usage for the active period."""
    _save_api_usage("tavily", _current_period(), used, TAVILY_FREE_CREDIT_LIMIT)


def tavily_web_search(query: str) -> str:
    """Search via the Tavily MCP server and return normalised results.

    Uses ``tavily_mcp()`` from the MCP server registry.
    Requires TAVILY_API_KEY to be set in .env.
    """
    if not API_KEYS.tavily_api_key:
        return "TAVILY_UNAVAILABLE: TAVILY_API_KEY is missing."

    async def _search() -> str:
        from swiss_exp_tracker.pipeline_agentic.mcp_servers import tavily_mcp

        server = tavily_mcp()
        try:
            async with server:
                result = await server.call_tool("tavily-search", {"query": query})
        except BaseExceptionGroup as eg:
            causes = "; ".join(str(e) for e in eg.exceptions)
            return f"TAVILY_UNAVAILABLE: request failed ({causes})."
        except Exception as exc:
            return f"TAVILY_UNAVAILABLE: request failed ({exc})."

        if not result.content:
            return "TAVILY_UNAVAILABLE: no results."

        lines: list[str] = []
        for item in result.content:
            text = getattr(item, "text", None)
            if text:
                stripped = str(text).strip()
                lower = stripped.lower()
                if any(
                    phrase in lower
                    for phrase in (
                        "credit limit exceeded",
                        "out of credits",
                        "quota exceeded",
                    )
                ):
                    return "TAVILY_CREDITS_EXCEEDED"
                if stripped:
                    lines.append(stripped)

        if not lines:
            return "TAVILY_UNAVAILABLE: no parseable results."

        return "TAVILY_RESULTS\n" + "\n".join(lines)

    return asyncio.run(_search())


def serpapi_web_search(query: str) -> str:
    """Search the web with SerpAPI. Returns fallback signal when key/quota is unavailable."""
    global _serpapi_quota_exceeded

    if _serpapi_quota_exceeded:
        return "SERPAPI_QUOTE_EXCEEDED"

    # Check monthly limit from DB
    serpapi_used = load_serpapi_usage()
    if serpapi_used >= SERPAPI_FREE_CREDIT_LIMIT:
        _serpapi_quota_exceeded = True
        return "SERPAPI_QUOTE_EXCEEDED"

    api_key = API_KEYS.serpapi_api_key
    if not api_key:
        return "SERPAPI_UNAVAILABLE: SERPAPI_API_KEY is missing."

    try:
        request_params: dict[str, str | int] = {
            "engine": "google",
            "q": query,
            "api_key": api_key,
            "num": 5,
            "hl": "de",
            "gl": "ch",
        }
        response = requests.get(
            "https://serpapi.com/search.json",
            params=request_params,
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return f"SERPAPI_UNAVAILABLE: request failed ({exc})."

    error_msg = str(payload.get("error", "")).lower()
    if error_msg:
        if "run out" in error_msg or "limit" in error_msg or "quota" in error_msg:
            _serpapi_quota_exceeded = True
            save_serpapi_usage(SERPAPI_FREE_CREDIT_LIMIT)
            return "SERPAPI_QUOTE_EXCEEDED"
        return f"SERPAPI_UNAVAILABLE: {payload.get('error')}"

    results = payload.get("organic_results", [])
    if not results:
        return "SERPAPI_UNAVAILABLE: no results"

    lines: list[str] = []
    for item in results[:5]:
        title = str(item.get("title", "")).strip()
        snippet = str(item.get("snippet", "")).strip()
        link = str(item.get("link", "")).strip()
        if title or snippet:
            lines.append(f"- {title} | {snippet} | {link}")

    if not lines:
        return "SERPAPI_UNAVAILABLE: no parseable results"

    save_serpapi_usage(serpapi_used + 1)
    return "SERPAPI_RESULTS\n" + "\n".join(lines)
