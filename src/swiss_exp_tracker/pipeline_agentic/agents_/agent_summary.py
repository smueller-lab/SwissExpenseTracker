from __future__ import annotations

import json
import os

from datetime import date
from typing import Any
from typing import cast

import requests

from agents import Agent
from agents import ModelSettings
from agents import function_tool
from dotenv import load_dotenv


load_dotenv(override=True)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


_TAVILY_FREE_CREDIT_LIMIT = 1000
_tavily_free_credits_used = 0
_serpapi_quota_exceeded = False
_TAVILY_USAGE_FILE = os.getenv(
    "TAVILY_USAGE_FILE",
    ".tavily_usage.json",
)


def _current_tavily_credit_period() -> str:
    """Return period key used to track free-credit usage across days.

    Default is monthly period (`YYYY-MM`) to align with typical billing cycles.
    Set `TAVILY_CREDIT_PERIOD=lifetime` to never auto-reset.
    """
    period_mode = os.getenv("TAVILY_CREDIT_PERIOD", "monthly").strip().lower()
    if period_mode == "lifetime":
        return "lifetime"
    return date.today().strftime("%Y-%m")


def _load_tavily_usage() -> int:
    """Load Tavily free-credit usage from disk for the active period."""
    try:
        with open(_TAVILY_USAGE_FILE, encoding="utf-8") as file_handle:
            raw_payload = json.load(file_handle)
    except FileNotFoundError:
        return 0
    except Exception:
        return 0

    if not isinstance(raw_payload, dict):
        return 0
    payload = cast("dict[str, Any]", raw_payload)

    period = str(payload.get("period", ""))
    used_raw = payload.get("used", 0)
    if period != _current_tavily_credit_period():
        return 0

    try:
        return max(0, int(used_raw))
    except Exception:
        return 0


def _save_tavily_usage(used: int) -> None:
    """Persist Tavily free-credit usage for the active period."""
    payload = {
        "period": _current_tavily_credit_period(),
        "used": max(0, int(used)),
        "limit": _TAVILY_FREE_CREDIT_LIMIT,
    }
    try:
        with open(_TAVILY_USAGE_FILE, "w", encoding="utf-8") as file_handle:
            json.dump(payload, file_handle)
    except Exception:
        return


def _tavily_api_search(query: str, payg_mode: bool) -> str:
    """Search via Tavily API and return normalized result/fallback signals."""
    api_key = os.getenv("TAVILY_API_KEY")
    if not api_key:
        return "TAVILY_UNAVAILABLE: TAVILY_API_KEY is missing."

    try:
        payload: dict[str, str | int | bool] = {
            "api_key": api_key,
            "query": query,
            "search_depth": "basic",
            "max_results": 5,
            "topic": "general",
            "include_answer": False,
            "include_raw_content": False,
        }
        if payg_mode:
            payload["auto_parameters"] = True

        response = requests.post(
            "https://api.tavily.com/search",
            json=payload,
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        return f"TAVILY_UNAVAILABLE: request failed ({exc})."

    error_msg = str(data.get("error", "") or "").lower()
    if error_msg:
        if "credit" in error_msg or "limit" in error_msg or "quota" in error_msg:
            return "TAVILY_CREDITS_EXCEEDED"
        return f"TAVILY_UNAVAILABLE: {data.get('error')}"

    results = data.get("results", [])
    if not results:
        return "TAVILY_UNAVAILABLE: no results"

    lines: list[str] = []
    for item in results[:5]:
        title = str(item.get("title", "")).strip()
        snippet = str(item.get("content", "")).strip()
        link = str(item.get("url", "")).strip()
        if title or snippet:
            lines.append(f"- {title} | {snippet} | {link}")

    if not lines:
        return "TAVILY_UNAVAILABLE: no parseable results"
    return "TAVILY_RESULTS\n" + "\n".join(lines)


def _serpapi_search_impl(query: str) -> str:
    """Search the web with SerpAPI. Returns fallback signal when key/quota is unavailable."""
    global _serpapi_quota_exceeded

    api_key = os.getenv("SERPAPI_API_KEY")
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
    return "SERPAPI_RESULTS\n" + "\n".join(lines)


@function_tool
def serpapi_search(query: str) -> str:
    """Search the web with SerpAPI."""
    return _serpapi_search_impl(query)


@function_tool
def search_with_fallback(query: str) -> str:
    """Search using Tavily first, then SerpAPI, then Tavily pay-as-you-go.

    Policy:
    1) Tavily free tier until configured credit budget is used.
    2) SerpAPI until SERPAPI_QUOTE_EXCEEDED.
    3) Tavily again in pay-as-you-go mode.
    """
    global _tavily_free_credits_used

    if _tavily_free_credits_used == 0:
        _tavily_free_credits_used = _load_tavily_usage()

    tavily_free_active = _tavily_free_credits_used < _TAVILY_FREE_CREDIT_LIMIT

    if tavily_free_active:
        tavily_result = _tavily_api_search(query, payg_mode=False)
        if tavily_result.startswith("TAVILY_RESULTS"):
            _tavily_free_credits_used += 1
            _save_tavily_usage(_tavily_free_credits_used)
            return (
                f"SEARCH_PROVIDER=tavily_free\n"
                f"TAVILY_FREE_CREDITS_USED={_tavily_free_credits_used}\n"
                + tavily_result
            )
        if tavily_result == "TAVILY_CREDITS_EXCEEDED":
            _tavily_free_credits_used = _TAVILY_FREE_CREDIT_LIMIT
            _save_tavily_usage(_tavily_free_credits_used)

    if not _serpapi_quota_exceeded:
        serp_result = _serpapi_search_impl(query)
        if serp_result.startswith("SERPAPI_RESULTS"):
            return "SEARCH_PROVIDER=serpapi\n" + serp_result
        if serp_result == "SERPAPI_QUOTE_EXCEEDED":
            payg_result = _tavily_api_search(query, payg_mode=True)
            return "SEARCH_PROVIDER=tavily_payg\n" + payg_result
        if serp_result.startswith("SERPAPI_UNAVAILABLE"):
            payg_result = _tavily_api_search(query, payg_mode=True)
            return "SEARCH_PROVIDER=tavily_payg\n" + payg_result

    payg_result = _tavily_api_search(query, payg_mode=True)
    return "SEARCH_PROVIDER=tavily_payg\n" + payg_result


BASE_INSTRUCTIONS = (
    "You are a helpful assistent. Given a Merchant, which can be a store, gas station, Restaurant, Bar, Golf Club, company, you will search the web for"
    "that term and give a concsie summary of that Merchant. The summary must contain following information:"
    "- Name of the Merchant"
    "- Location of the Merchant"
    "- What the merchant is doing or selling: What is the main business? Do they have side products or services?"
    "- If possible search for typical products or services they offer"
    "- What are typical characteristics of the customers."
    "- How many stores do they have?"
    "Please return only a summary of the points and no information which isn't necessary for the summary."
    "The idea behind the summary to get a good overview about how the Merchant could be categorized."
    "Keep in mind that most transactions are coming from switzerland"
)


def get_instructions_with_tools_use(base_instructions: str) -> tuple[str, list[Any]]:
    use_normal_websearch = _env_flag("USE_NORMAL_WEBSEARCH", default=False)
    if use_normal_websearch:
        return (
            base_instructions
            + "Always call search_with_fallback exactly once before writing the summary."
        ), [search_with_fallback]
    return (
        base_instructions
        + "Always call search_with_fallback exactly once before writing the summary."
        + "This tool already applies policy: Tavily free credits first, then SerpAPI until SERPAPI_QUOTE_EXCEEDED, then Tavily pay-as-you-go."
    ), [search_with_fallback]


INSTRUCTIONS, TOOLS = get_instructions_with_tools_use(BASE_INSTRUCTIONS)

summary_agent = Agent(
    name="Summary Agent",
    instructions=INSTRUCTIONS,
    tools=TOOLS,
    model="gpt-4o-mini",
    model_settings=ModelSettings(tool_choice="required"),
)
