"""MCP server registry.

All remote MCP servers used by this project are defined here.
This is the single place to see, add, or change MCP server connections.

Servers
-------
- Bright Data Web Search  https://mcp.brightdata.com
- Tavily Web Search       https://mcp.tavily.com
"""

from __future__ import annotations

from agents.mcp import MCPServerStreamableHttp
from agents.mcp import MCPServerStreamableHttpParams

from swiss_exp_tracker.pipeline_agentic.libs import API_KEYS


def bright_data_mcp() -> MCPServerStreamableHttp:
    """Bright Data remote MCP server.

    Exposes the ``search_engine`` tool — general web search with
    a free allowance of 5 000 requests / month.

    Docs: https://github.com/luminati-io/mcp
    """
    return MCPServerStreamableHttp(
        name="Bright Data Web Search",
        params=MCPServerStreamableHttpParams(
            url=f"https://mcp.brightdata.com/mcp?token={API_KEYS.bright_data_api_key}",
            timeout=60.0,  # Bright Data can be slow; default 5 s causes 504s
        ),
        cache_tools_list=True,
        client_session_timeout_seconds=60.0,
        max_retry_attempts=2,
        retry_backoff_seconds_base=2.0,
        tool_filter={"allowed_tool_names": ["search_engine"]},
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
