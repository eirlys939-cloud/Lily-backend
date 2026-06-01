"""
Celyn's Memory MCP 客户端
- 用官方 mcp SDK 的 streamable-http transport
- 每次调用建立短连接（个人项目流量极小，无需常驻 session）
"""
from __future__ import annotations

import logging
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

log = logging.getLogger("lily.memory")


class CelynMemoryClient:
    def __init__(self, url: str, bearer: str):
        self.url = url
        self.headers = {"Authorization": f"Bearer {bearer}"} if bearer else {}

    async def list_tools(self) -> list[dict[str, Any]]:
        """返回 Anthropic tools 格式的工具 schema 列表。"""
        async with streamablehttp_client(self.url, headers=self.headers) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.list_tools()
                tools = []
                for t in result.tools:
                    tools.append({
                        "name": f"memory_{t.name}",  # 命名空间前缀，避免和 Notion 工具冲突
                        "description": t.description or "",
                        "input_schema": t.inputSchema or {"type": "object", "properties": {}},
                        # 内部记录原始名，调用时还原
                        "_original_name": t.name,
                    })
                return tools

    async def call_tool(self, original_name: str, arguments: dict[str, Any]) -> str:
        """调用一个工具，返回字符串化的结果（喂给 Claude）。"""
        async with streamablehttp_client(self.url, headers=self.headers) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(original_name, arguments=arguments)
                # result.content 是 list[TextContent | ImageContent | ...]
                parts: list[str] = []
                for c in result.content:
                    if hasattr(c, "text") and c.text:
                        parts.append(c.text)
                    else:
                        parts.append(str(c))
                text = "\n".join(parts) if parts else "(empty)"
                if result.isError:
                    return f"[memory tool error] {text}"
                return text
