from __future__ import annotations

import os
import re
import urllib.parse

from datetime import date
from datetime import datetime

import requests

from dotenv import load_dotenv
from pydantic import BaseModel

from swiss_exp_tracker.db.sql import transactions
from swiss_exp_tracker.pipeline_ingestion.db import get_connection

load_dotenv(override=True)


class ApiKeys(BaseModel):
    tavily_api_key: str | None = None
    exa_api_key: str | None = None
    brave_search_api_key: str | None = None
    scrape_do_api_key: str | None = None


def get_settings() -> ApiKeys:
    """Load API keys and other settings from environment variables."""
    return ApiKeys(
        tavily_api_key=os.getenv("TAVILY_API_KEY"),
        exa_api_key=os.getenv("EXA_API_KEY"),
        brave_search_api_key=os.getenv("BRAVE_SEARCH_API_KEY"),
        scrape_do_api_key=os.getenv("SCRAPE_DO_API_KEY"),
    )


API_KEYS = get_settings()


# ── Tavily ───────────────────────────────────────────────────────────────────
TAVILY_FREE_CREDIT_LIMIT: int = 1000

# ── Exa ──────────────────────────────────────────────────────────────────────
EXA_FREE_CREDIT_LIMIT: int = 1000

# ── Brave Search ─────────────────────────────────────────────────────────────
BRAVE_SEARCH_FREE_CREDIT_LIMIT: int = 1000

# ── Scrape.do ────────────────────────────────────────────────────────────────
SCRAPE_DO_FREE_CREDIT_LIMIT: int = 1000


# ── API usage persistence (SQLite) ───────────────────────────────────────────


def _current_period() -> str:
    """Shared monthly period key (YYYY-MM) used for all provider credit buckets."""
    return date.today().strftime("%Y-%m")


def _load_api_usage(provider: str, period: str) -> int:
    """Return the stored *used* count for *provider* in *period*, or 0."""
    with get_connection() as db:
        used = transactions.get_api_usage(db, provider=provider, period=period)
    return used if used is not None else 0


def _save_api_usage(provider: str, period: str, used: int, credit_limit: int) -> None:
    """Upsert the *used* count for *provider* / *period* into the DB."""
    try:
        with get_connection() as db:
            transactions.upsert_api_usage(
                db,
                provider=provider,
                period=period,
                used=max(0, int(used)),
                credit_limit=credit_limit,
                updated_at=datetime.now().astimezone().isoformat(),
            )
    except Exception:
        return None


def load_exa_usage() -> int:
    """Load Exa free-credit usage from the DB for the current monthly period."""
    return _load_api_usage("exa", _current_period())


def save_exa_usage(used: int) -> None:
    """Persist Exa free-credit usage for the current monthly period."""
    _save_api_usage("exa", _current_period(), used, EXA_FREE_CREDIT_LIMIT)


def exa_web_search(query: str) -> str:
    """Search via the Exa REST API directly (no MCP overhead).

    Requires EXA_API_KEY to be set in .env.
    """
    if not API_KEYS.exa_api_key:
        return "EXA_UNAVAILABLE: EXA_API_KEY is missing."

    try:
        response = requests.post(
            "https://api.exa.ai/search",
            headers={
                "x-api-key": API_KEYS.exa_api_key,
                "Content-Type": "application/json",
            },
            json={
                "query": query,
                "type": "auto",
                "numResults": 5,
                "contents": {
                    "summary": {"query": query},
                },
            },
            timeout=15.0,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return f"EXA_UNAVAILABLE: request failed ({exc})."

    error = str(payload.get("error", "")).lower()
    if error:
        if any(p in error for p in ("credit", "quota", "limit", "exceeded")):
            return "EXA_CREDITS_EXCEEDED"
        return f"EXA_UNAVAILABLE: {payload.get('error')}"

    results = payload.get("results", [])
    if not results:
        return "EXA_UNAVAILABLE: no results."

    lines: list[str] = []
    for r in results:
        title = r.get("title", "")
        summary = r.get("summary", "") or r.get("text", "") or r.get("snippet", "")
        url = r.get("url", "")
        entry = f"{title}: {summary} ({url})" if summary else f"{title} ({url})"
        if title or url:
            lines.append(entry)

    if not lines:
        return "EXA_UNAVAILABLE: no parseable results."

    return f"EXA_RESULTS:{len(lines)}\n" + "\n".join(lines)


def load_brave_usage() -> int:
    """Load Brave Search free-credit usage from the DB for the current monthly period."""
    return _load_api_usage("brave_search", _current_period())


def save_brave_usage(used: int) -> None:
    """Persist Brave Search free-credit usage for the current monthly period."""
    _save_api_usage(
        "brave_search", _current_period(), used, BRAVE_SEARCH_FREE_CREDIT_LIMIT
    )


def load_scrape_do_usage() -> int:
    """Load Scrape.do free-credit usage from the DB for the current monthly period."""
    return _load_api_usage("scrape_do", _current_period())


def save_scrape_do_usage(used: int) -> None:
    """Persist Scrape.do free-credit usage for the current monthly period."""
    _save_api_usage("scrape_do", _current_period(), used, SCRAPE_DO_FREE_CREDIT_LIMIT)


def brave_web_search(query: str) -> str:
    """Search via the Brave Search REST API.

    Requires BRAVE_SEARCH_API_KEY to be set in .env.
    Docs: https://api.search.brave.com/app/documentation/web-search/get-started
    """
    if not API_KEYS.brave_search_api_key:
        return "BRAVE_UNAVAILABLE: BRAVE_SEARCH_API_KEY is missing."

    try:
        response = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "X-Subscription-Token": API_KEYS.brave_search_api_key,
            },
            params={"q": query, "count": "5"},
            timeout=10.0,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return f"BRAVE_UNAVAILABLE: request failed ({exc})."

    error = str(payload.get("message", "") or payload.get("error", "")).lower()
    if error:
        if any(p in error for p in ("credit", "quota", "limit", "exceeded", "rate")):
            return "BRAVE_CREDITS_EXCEEDED"
        return f"BRAVE_UNAVAILABLE: {error}"

    results = payload.get("web", {}).get("results", [])
    if not results:
        return "BRAVE_UNAVAILABLE: no results."

    lines: list[str] = []
    for r in results:
        title = r.get("title", "")
        description = r.get("description", "")
        url = r.get("url", "")
        if description:
            lines.append(f"{title}: {description} ({url})")

    if not lines:
        return "BRAVE_UNAVAILABLE: no parseable results."

    return "BRAVE_RESULTS\n" + "\n".join(lines)


# ── Tavily credit persistence (DB-backed) ────────────────────────────────────


def load_tavily_usage() -> int:
    """Load Tavily free-credit usage from the DB for the active period."""
    return _load_api_usage("tavily", _current_period())


def save_tavily_usage(used: int) -> None:
    """Persist Tavily free-credit usage for the active period."""
    _save_api_usage("tavily", _current_period(), used, TAVILY_FREE_CREDIT_LIMIT)


def tavily_web_search(query: str) -> str:
    """Search via the Tavily REST API directly (no MCP overhead).

    Requires TAVILY_API_KEY to be set in .env.
    """
    if not API_KEYS.tavily_api_key:
        return "TAVILY_UNAVAILABLE: TAVILY_API_KEY is missing."

    try:
        response = requests.post(
            "https://api.tavily.com/search",
            json={
                "api_key": API_KEYS.tavily_api_key,
                "query": query,
                "search_depth": "basic",
                "max_results": 5,
            },
            timeout=5.0,  # fail fast — if Tavily is slow, fall through to SerpAPI
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return f"TAVILY_UNAVAILABLE: request failed ({exc})."

    error = str(payload.get("error", "")).lower()
    if error:
        if any(p in error for p in ("credit", "quota", "limit", "exceeded")):
            return "TAVILY_CREDITS_EXCEEDED"
        return f"TAVILY_UNAVAILABLE: {payload.get('error')}"

    results = payload.get("results", [])
    if not results:
        return "TAVILY_UNAVAILABLE: no results."

    lines: list[str] = []
    for r in results:
        title = r.get("title", "")
        content = r.get("content", "")
        url = r.get("url", "")
        if content:
            lines.append(f"{title}: {content} ({url})")

    if not lines:
        return "TAVILY_UNAVAILABLE: no parseable results."

    return "TAVILY_RESULTS\n" + "\n".join(lines)


def scrape_do_web_search(query: str) -> str:
    """Search the web via Scrape.do by scraping DuckDuckGo HTML results.

    Uses the full DuckDuckGo HTML endpoint (not the lite version) to get
    proper title + snippet results.

    Requires SCRAPE_DO_API_KEY to be set in .env.
    Docs: https://scrape.do/documentation/
    """
    if not API_KEYS.scrape_do_api_key:
        return "SCRAPEDO_UNAVAILABLE: SCRAPE_DO_API_KEY is missing."

    search_url = (
        "https://html.duckduckgo.com/html/?q="
        + urllib.parse.quote(query)
        + "&kl=ch-en"  # Swiss locale for better regional relevance
    )

    try:
        response = requests.get(
            "https://api.scrape.do",
            params={
                "token": API_KEYS.scrape_do_api_key,
                "url": search_url,
            },
            timeout=30.0,
        )
        response.raise_for_status()
        html = response.text
    except Exception as exc:
        return f"SCRAPEDO_UNAVAILABLE: request failed ({exc})."

    lower_html = html.lower()
    credit_exhausted_phrases = (
        "credit limit exceeded",
        "credits exceeded",
        "out of credits",
        "quota exceeded",
        "monthly quota",
        "rate limit exceeded",
    )
    if any(phrase in lower_html for phrase in credit_exhausted_phrases):
        return "SCRAPEDO_CREDITS_EXCEEDED"

    def _clean(s: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", s)).strip()

    # DuckDuckGo HTML structure: result__a (title), result__snippet, result__url
    titles = re.findall(r'class="result__a"[^>]*>(.*?)</a>', html, re.DOTALL)
    snippets = re.findall(r'class="result__snippet"[^>]*>(.*?)</a>', html, re.DOTALL)
    urls = re.findall(r'class="result__url"[^>]*>(.*?)</span>', html, re.DOTALL)

    lines: list[str] = []
    for i, title in enumerate(titles[:5]):
        t = _clean(title)
        s = _clean(snippets[i]) if i < len(snippets) else ""
        u = _clean(urls[i]) if i < len(urls) else ""
        entry = f"{t}: {s} ({u})" if s else f"{t} ({u})"
        if t:
            lines.append(entry)

    if not lines:
        # Fallback: strip all HTML tags and return first 2000 chars
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return "SCRAPEDO_UNAVAILABLE: no parseable results."
        lines = [text[:2000]]

    return "SCRAPEDO_RESULTS\n" + "\n".join(lines)
