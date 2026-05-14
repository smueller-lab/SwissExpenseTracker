"""MCP server registry.

All remote MCP servers used by this project are defined here.
This is the single place to see, add, or change MCP server connections.

Servers
-------
- Exa Web Search          https://mcp.exa.ai/mcp
- Tavily Web Search       https://mcp.tavily.com
"""

from __future__ import annotations

from agents.mcp import MCPServerStreamableHttp
from agents.mcp import MCPServerStreamableHttpParams

from swiss_exp_tracker.pipeline_agentic.libs import API_KEYS


def exa_mcp() -> MCPServerStreamableHttp:
    """Exa remote MCP server.

    Exposes web search tools — fastest option with a free allowance of
    1 000 requests / month.

    Docs: https://docs.exa.ai/docs/mcp-server
    """
    return MCPServerStreamableHttp(
        name="Exa Web Search",
        params=MCPServerStreamableHttpParams(
            url="https://mcp.exa.ai/mcp",
            headers={"x-api-key": API_KEYS.exa_api_key or ""},
            timeout=30.0,
        ),
        cache_tools_list=True,
        client_session_timeout_seconds=30.0,
        max_retry_attempts=2,
        retry_backoff_seconds_base=2.0,
    )


def tavily_mcp() -> MCPServerStreamableHttp:
    """Tavily remote MCP server.

    Exposes the ``tavily-search`` tool — AI-optimised web search with
    a free allowance of 1 000 requests / month.

    Docs: https://docs.tavily.com/documentation/mcp
    """
    return MCPServerStreamableHttp(
        name="Tavily Web Search",
        params=MCPServerStreamableHttpParams(
            url=f"https://mcp.tavily.com/mcp?apikey={API_KEYS.tavily_api_key}",
            timeout=30.0,
        ),
        cache_tools_list=True,
        client_session_timeout_seconds=30.0,
        max_retry_attempts=2,
        retry_backoff_seconds_base=2.0,
        tool_filter={"allowed_tool_names": ["tavily-search"]},
    )
