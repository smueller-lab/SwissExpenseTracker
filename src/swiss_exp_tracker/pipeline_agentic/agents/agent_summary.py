from __future__ import annotations

import os

from typing import Any

import requests

from agents import Agent
from agents import ModelSettings
from agents import WebSearchTool
from agents import function_tool
from dotenv import load_dotenv


load_dotenv(override=True)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@function_tool
def serpapi_search(query: str) -> str:
    """Search the web with SerpAPI. Returns fallback signal when key/quota is unavailable."""
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
            return "SERPAPI_QUOTA_EXCEEDED"
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
        return base_instructions + "Always call WebSearchTool.", [
            WebSearchTool(search_context_size="low")
        ]
    else:
        return (
            base_instructions
            + "Always call tool serpapi_search first."
            + "If serpapi_search returns SERPAPI_QUOTA_EXCEEDED or SERPAPI_UNAVAILABLE, call WebSearchTool as fallback."
        ), [serpapi_search, WebSearchTool(search_context_size="low")]


INSTRUCTIONS, TOOLS = get_instructions_with_tools_use(BASE_INSTRUCTIONS)

summary_agent = Agent(
    name="Summary Agent",
    instructions=INSTRUCTIONS,
    tools=TOOLS,
    model="gpt-4o-mini",
    model_settings=ModelSettings(tool_choice="required"),
)
