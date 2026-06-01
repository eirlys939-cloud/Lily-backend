"""
Notion 工具集
- 用 Internal Integration Token 直接调 https://api.notion.com/v1/*
- 不走 OAuth，不依赖远程 Notion MCP server
- 暴露 5 个核心工具，覆盖 90% 场景：
    notion_search          搜索页面/数据库
    notion_query_database  查询数据库（带过滤、排序）
    notion_get_page        获取页面内容（含 blocks）
    notion_create_page     创建页面（在父页面/数据库下）
    notion_append_blocks   向页面追加内容块（最常用：写日记/写温度）
"""
from __future__ import annotations

import json
import logging
from typing import Any

import httpx

log = logging.getLogger("lily.notion")
NOTION_API = "https://api.notion.com/v1"


class NotionTools:
    def __init__(self, token: str, version: str = "2022-06-28"):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": version,
            "Content-Type": "application/json",
        }

    # ============ tool schemas（喂给 Claude） ============

    def tool_schemas(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "notion_search",
                "description": (
                    "在你能访问到的 Notion 工作区里搜索页面或数据库。"
                    "用关键词找到目标后，再用 notion_get_page 或 notion_query_database 读详细内容。"
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "搜索关键词，可空表示列出全部可访问对象"},
                        "filter_type": {
                            "type": "string",
                            "enum": ["page", "database"],
                            "description": "可选：只搜页面或只搜数据库",
                        },
                        "page_size": {"type": "integer", "default": 20, "description": "返回数量，默认 20"},
                    },
                },
            },
            {
                "name": "notion_query_database",
                "description": (
                    "查询一个数据库的所有条目（行）。可选过滤和排序。"
                    "返回每行的 properties 摘要。常用于浏览 Warmth、Todo 等数据库。"
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "database_id": {"type": "string", "description": "数据库 ID（UUID，可带或不带横杠）"},
                        "filter": {
                            "type": "object",
                            "description": "Notion 原生 filter 对象，详见官方文档。可省略。",
                        },
                        "sorts": {
                            "type": "array",
                            "description": "Notion 原生 sorts 数组，可省略",
                            "items": {"type": "object"},
                        },
                        "page_size": {"type": "integer", "default": 50},
                    },
                    "required": ["database_id"],
                },
            },
            {
                "name": "notion_get_page",
                "description": (
                    "获取一个页面的完整内容：properties + 所有正文 blocks。"
                    "用于读取一篇温度日记、一条 todo 详情等。"
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "page_id": {"type": "string", "description": "页面 ID"},
                    },
                    "required": ["page_id"],
                },
            },
            {
                "name": "notion_create_page",
                "description": (
                    "新建一个页面。父级可以是数据库（在数据库里建一行）或另一个页面（在页面下建子页）。"
                    "title 必填；children 是 markdown 字符串（自动转换成 blocks），也可以直接传 blocks 数组。"
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "parent_type": {
                            "type": "string",
                            "enum": ["database", "page"],
                            "description": "父级类型",
                        },
                        "parent_id": {"type": "string", "description": "父数据库 ID 或父页面 ID"},
                        "title": {"type": "string", "description": "页面标题"},
                        "properties": {
                            "type": "object",
                            "description": (
                                "数据库行的其他字段。键是字段名，值是 Notion 原生 property value 对象。"
                                "例如 {\"标签\": {\"multi_select\":[{\"name\":\"日常\"}]}}。"
                                "title 字段不要在这里填，用上面的 title 参数。"
                            ),
                        },
                        "markdown": {
                            "type": "string",
                            "description": "正文，markdown 字符串。会被切分成段落 block 写入。",
                        },
                    },
                    "required": ["parent_type", "parent_id", "title"],
                },
            },
            {
                "name": "notion_append_blocks",
                "description": (
                    "向某个已存在的页面尾部追加内容（markdown）。常用：往日记页加新段落。"
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "page_id": {"type": "string"},
                        "markdown": {"type": "string", "description": "要追加的 markdown 文本"},
                    },
                    "required": ["page_id", "markdown"],
                },
            },
        ]

    # ============ 调度入口 ============

    async def call(self, name: str, arguments: dict[str, Any]) -> str:
        try:
            if name == "notion_search":
                return await self._search(**arguments)
            if name == "notion_query_database":
                return await self._query_db(**arguments)
            if name == "notion_get_page":
                return await self._get_page(**arguments)
            if name == "notion_create_page":
                return await self._create_page(**arguments)
            if name == "notion_append_blocks":
                return await self._append_blocks(**arguments)
            return f"[notion] unknown tool: {name}"
        except httpx.HTTPStatusError as e:
            return f"[notion error {e.response.status_code}] {e.response.text}"
        except Exception as e:
            log.exception("notion tool error")
            return f"[notion error] {type(e).__name__}: {e}"

    # ============ 实现 ============

    async def _search(self, query: str = "", filter_type: str | None = None, page_size: int = 20) -> str:
        payload: dict[str, Any] = {"query": query, "page_size": page_size}
        if filter_type:
            payload["filter"] = {"value": filter_type, "property": "object"}
        async with httpx.AsyncClient(timeout=30) as cli:
            r = await cli.post(f"{NOTION_API}/search", headers=self.headers, json=payload)
            r.raise_for_status()
            data = r.json()
        items = []
        for it in data.get("results", []):
            obj = it.get("object")
            title = self._extract_title(it)
            items.append({
                "id": it["id"],
                "object": obj,
                "title": title,
                "url": it.get("url"),
            })
        return json.dumps({"count": len(items), "results": items}, ensure_ascii=False, indent=2)

    async def _query_db(
        self,
        database_id: str,
        filter: dict[str, Any] | None = None,
        sorts: list[dict[str, Any]] | None = None,
        page_size: int = 50,
    ) -> str:
        payload: dict[str, Any] = {"page_size": page_size}
        if filter:
            payload["filter"] = filter
        if sorts:
            payload["sorts"] = sorts
        async with httpx.AsyncClient(timeout=30) as cli:
            r = await cli.post(
                f"{NOTION_API}/databases/{database_id}/query",
                headers=self.headers,
                json=payload,
            )
            r.raise_for_status()
            data = r.json()
        rows = []
        for row in data.get("results", []):
            rows.append({
                "id": row["id"],
                "url": row.get("url"),
                "properties": self._simplify_properties(row.get("properties", {})),
            })
        return json.dumps({"count": len(rows), "rows": rows}, ensure_ascii=False, indent=2)

    async def _get_page(self, page_id: str) -> str:
        async with httpx.AsyncClient(timeout=30) as cli:
            r1 = await cli.get(f"{NOTION_API}/pages/{page_id}", headers=self.headers)
            r1.raise_for_status()
            page = r1.json()

            r2 = await cli.get(
                f"{NOTION_API}/blocks/{page_id}/children",
                headers=self.headers,
                params={"page_size": 100},
            )
            r2.raise_for_status()
            blocks_data = r2.json()

        text_parts: list[str] = []
        for b in blocks_data.get("results", []):
            text_parts.append(self._block_to_text(b))

        return json.dumps({
            "id": page["id"],
            "url": page.get("url"),
            "properties": self._simplify_properties(page.get("properties", {})),
            "content": "\n".join(t for t in text_parts if t),
        }, ensure_ascii=False, indent=2)

    async def _create_page(
        self,
        parent_type: str,
        parent_id: str,
        title: str,
        properties: dict[str, Any] | None = None,
        markdown: str | None = None,
    ) -> str:
        if parent_type == "database":
            parent = {"database_id": parent_id}
            props = dict(properties or {})
            # 数据库行的 title 需要找到 title 字段名——我们假设字段叫 "Name"/"名称"/"标题"，
            # 实际上 Notion 数据库可以叫任意名字。所以我们把 title 塞进 properties 里时，
            # 让模型自己在 properties 参数里指定 title。这里如果 properties 里没有任何 title 类型字段，
            # 就尝试用 "名称" 或 "Name" 作为兜底。
            has_title = any(
                isinstance(v, dict) and "title" in v
                for v in props.values()
            )
            if not has_title:
                # 兜底：尝试 "名称" 和 "Name"
                props.setdefault("名称", {"title": [{"text": {"content": title}}]})
                # 如果模型用了 Name 字段：
                # 实际上数据库结构是确定的，这里兜底覆盖率不一定够，但能跑通常见场景
        else:  # page
            parent = {"page_id": parent_id}
            props = {"title": {"title": [{"text": {"content": title}}]}}

        payload: dict[str, Any] = {"parent": parent, "properties": props}
        if markdown:
            payload["children"] = self._markdown_to_blocks(markdown)

        async with httpx.AsyncClient(timeout=30) as cli:
            r = await cli.post(f"{NOTION_API}/pages", headers=self.headers, json=payload)
            r.raise_for_status()
            data = r.json()
        return json.dumps({
            "ok": True,
            "id": data["id"],
            "url": data.get("url"),
        }, ensure_ascii=False)

    async def _append_blocks(self, page_id: str, markdown: str) -> str:
        blocks = self._markdown_to_blocks(markdown)
        async with httpx.AsyncClient(timeout=30) as cli:
            r = await cli.patch(
                f"{NOTION_API}/blocks/{page_id}/children",
                headers=self.headers,
                json={"children": blocks},
            )
            r.raise_for_status()
        return json.dumps({"ok": True, "appended": len(blocks)}, ensure_ascii=False)

    # ============ helpers ============

    @staticmethod
    def _extract_title(item: dict[str, Any]) -> str:
        # 页面：properties 里找 title 类型字段
        # 数据库：title 字段在顶层
        if item.get("object") == "database":
            return "".join(t.get("plain_text", "") for t in item.get("title", []))
        props = item.get("properties", {})
        for v in props.values():
            if isinstance(v, dict) and v.get("type") == "title":
                return "".join(t.get("plain_text", "") for t in v.get("title", []))
        return "(no title)"

    @staticmethod
    def _simplify_properties(props: dict[str, Any]) -> dict[str, Any]:
        """把 Notion 复杂的 property 结构压扁成人类可读。"""
        out = {}
        for k, v in props.items():
            t = v.get("type")
            if t == "title":
                out[k] = "".join(x.get("plain_text", "") for x in v.get("title", []))
            elif t == "rich_text":
                out[k] = "".join(x.get("plain_text", "") for x in v.get("rich_text", []))
            elif t == "select":
                out[k] = (v.get("select") or {}).get("name")
            elif t == "multi_select":
                out[k] = [x.get("name") for x in v.get("multi_select", [])]
            elif t == "date":
                d = v.get("date") or {}
                out[k] = d.get("start") if d else None
            elif t == "number":
                out[k] = v.get("number")
            elif t == "checkbox":
                out[k] = v.get("checkbox")
            elif t == "url":
                out[k] = v.get("url")
            elif t == "people":
                out[k] = [p.get("name") for p in v.get("people", [])]
            else:
                out[k] = v.get(t)
        return out

    @staticmethod
    def _block_to_text(block: dict[str, Any]) -> str:
        t = block.get("type")
        if not t:
            return ""
        body = block.get(t, {})
        rich = body.get("rich_text", [])
        text = "".join(x.get("plain_text", "") for x in rich)
        if t == "heading_1":
            return f"# {text}"
        if t == "heading_2":
            return f"## {text}"
        if t == "heading_3":
            return f"### {text}"
        if t == "bulleted_list_item":
            return f"- {text}"
        if t == "numbered_list_item":
            return f"1. {text}"
        if t == "to_do":
            checked = body.get("checked", False)
            return f"[{'x' if checked else ' '}] {text}"
        if t == "quote":
            return f"> {text}"
        if t == "code":
            lang = body.get("language", "")
            return f"```{lang}\n{text}\n```"
        return text

    @staticmethod
    def _markdown_to_blocks(md: str) -> list[dict[str, Any]]:
        """极简 markdown → Notion blocks 转换。支持 # 标题、- 列表、空行分段。"""
        blocks: list[dict[str, Any]] = []
        for raw in md.split("\n"):
            line = raw.rstrip()
            if not line.strip():
                continue
            if line.startswith("### "):
                blocks.append({"object": "block", "type": "heading_3", "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": line[4:]}}]
                }})
            elif line.startswith("## "):
                blocks.append({"object": "block", "type": "heading_2", "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": line[3:]}}]
                }})
            elif line.startswith("# "):
                blocks.append({"object": "block", "type": "heading_1", "heading_1": {
                    "rich_text": [{"type": "text", "text": {"content": line[2:]}}]
                }})
            elif line.startswith("- "):
                blocks.append({"object": "block", "type": "bulleted_list_item", "bulleted_list_item": {
                    "rich_text": [{"type": "text", "text": {"content": line[2:]}}]
                }})
            elif line.startswith("> "):
                blocks.append({"object": "block", "type": "quote", "quote": {
                    "rich_text": [{"type": "text", "text": {"content": line[2:]}}]
                }})
            else:
                blocks.append({"object": "block", "type": "paragraph", "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": line}}]
                }})
        return blocks
