"""
工具循环：tool_use ↔ tool_result 多轮，流式 SSE 输出。

工作流：
1. 把 messages + tools 发给上游（msuicode /v1/messages，stream=True）
2. 流式解析事件，把文本增量实时 yield 给前端
3. 遇到 tool_use block：完整收集 → 并发执行（Memory MCP / Notion）→ 喂回 messages
4. 如果 stop_reason == "tool_use"，回到步骤 1 再来一轮；否则结束

【Prompt Caching 说明】
- system prompt：自动包装成数组结构并打 cache_control（前端传字符串即可）
- tools 数组：在最后一个工具上打 cache_control（整个工具列表会被缓存）
- metadata.user_id：固定为 "lily-celyn"，保证请求路由到同一台服务器
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncGenerator

import httpx

from config import settings
from mcp_client import CelynMemoryClient
from notion_tools import NotionTools

log = logging.getLogger("lily.loop")


def _sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


async def _build_tool_specs(
    memory_client: CelynMemoryClient,
    notion: NotionTools,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """
    返回 (Anthropic tools 数组, 工具路由表)
    路由表 key 是工具名（前缀化后），value 是 {"kind":"memory","original":"breath"} 或 {"kind":"notion"}
    """
    tools_for_api: list[dict[str, Any]] = []
    routes: dict[str, dict[str, Any]] = {}

    # Memory MCP 工具
    try:
        mem_tools = await memory_client.list_tools()
    except Exception as e:
        log.warning(f"无法获取 Memory 工具，本轮跳过：{e}")
        mem_tools = []
    for t in mem_tools:
        name = t["name"]
        tools_for_api.append({
            "name": name,
            "description": t["description"],
            "input_schema": t["input_schema"],
        })
        routes[name] = {"kind": "memory", "original": t["_original_name"]}

    # Notion 工具
    for t in notion.tool_schemas():
        tools_for_api.append(t)
        routes[t["name"]] = {"kind": "notion"}

    # ★ 注意：tools 上不挂 cache_control。
    #   原因：Anthropic 每个请求最多 4 个 cache_control 标记。我们把槽位留给：
    #     1) system（稳定）
    #     2) messages 里的 BP4 rolling 锚点
    #   tools 字段本身每轮都会包含在请求里，只要 system 命中，前缀就会延伸到
    #   tools 自然命中，不需要单独挂标。多挂反而可能让中转站"分段建缓存"，
    #   导致每段都按 write 计费。

    return tools_for_api, routes


async def _dispatch_tool(
    name: str,
    arguments: dict[str, Any],
    routes: dict[str, dict[str, Any]],
    memory_client: CelynMemoryClient,
    notion: NotionTools,
) -> str:
    route = routes.get(name)
    if not route:
        return f"[error] unknown tool: {name}"
    if route["kind"] == "memory":
        return await memory_client.call_tool(route["original"], arguments)
    if route["kind"] == "notion":
        return await notion.call(name, arguments)
    return f"[error] unhandled route kind for {name}"


async def run_tool_loop(
    *,
    model: str,
    messages: list[dict[str, Any]],
    system: str | list[dict[str, Any]] | None,
    max_tokens: int,
    temperature: float,
    thinking: dict[str, Any] | None,
    memory_client: CelynMemoryClient,
    notion: NotionTools,
) -> AsyncGenerator[str, None]:
    """主循环。yields SSE 字符串。"""

    tools, routes = await _build_tool_specs(memory_client, notion)
    log.info(f"工具就绪：共 {len(tools)} 个（Memory + Notion）")

    # 复制 messages，准备 in-place 追加 assistant / tool_result
    convo: list[dict[str, Any]] = list(messages)

    def _apply_rolling_cache_breakpoint(msgs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        ★ BP4 稳定 rolling 断点：在 messages 数组里挂一个 cache_control，
        把缓存边界从 system 末尾扩展到包含历史对话。

        关键设计：BP4 位置不每轮都动，否则 msuicode 会把每轮当新缓存来写。
        策略：挂在 user[k]，其中 k = ((len(user_indices) - 2) // 2) * 2
        效果：
            user 数 = 2 → k=0（挂第1条 user）
            user 数 = 3 → k=0（不动，复用上轮缓存）
            user 数 = 4 → k=2（挪一格，触发一次重写后稳定）
            user 数 = 5 → k=2（不动）
            user 数 = 6 → k=4（挪一格）
        这样每两轮才挪一次，连续轮次之间能高命中。
        """
        user_indices = [i for i, m in enumerate(msgs) if m.get("role") == "user"]
        if len(user_indices) < 2:
            return msgs  # 不够两条 user，没法打 rolling 标

        # 稳定锚点：每两轮挪一次
        anchor_step = ((len(user_indices) - 2) // 2) * 2
        target_idx = user_indices[anchor_step]

        result = list(msgs)
        target_msg = dict(result[target_idx])
        content = target_msg.get("content")

        if isinstance(content, str):
            # 字符串 content：包装成 content block 数组
            target_msg["content"] = [{
                "type": "text",
                "text": content,
                "cache_control": {"type": "ephemeral", "ttl": "1h"},
            }]
        elif isinstance(content, list) and content:
            # 已经是 content block 数组：给最后一个 block 打标
            new_content = list(content)
            last_block = dict(new_content[-1])
            last_block["cache_control"] = {"type": "ephemeral", "ttl": "1h"}
            new_content[-1] = last_block
            target_msg["content"] = new_content
        else:
            return msgs  # content 为空或异常，跳过

        result[target_idx] = target_msg
        log.info(f"  BP4 锚点：user[{anchor_step}] / 共 {len(user_indices)} 条 user")
        return result

    for round_idx in range(settings.MAX_TOOL_ROUNDS):
        log.info(f"=== 工具循环第 {round_idx + 1} 轮 ===")
        # ★ 在发送前打 rolling 断点
        convo_for_send = _apply_rolling_cache_breakpoint(convo)
        payload: dict[str, Any] = {
            "model": model,
            "messages": convo_for_send,
            "max_tokens": max_tokens,
            "stream": True,
            "tools": tools,
        }
        # ★ metadata.user_id：msuicode 中转站可能不支持，用环境变量开关
        #   想开启时在 Zeabur 设 ENABLE_CACHE_METADATA=1
        if getattr(settings, "ENABLE_CACHE_METADATA", False):
            payload["metadata"] = {"user_id": "lily-celyn"}
        # 思考链：开启时 Anthropic 要求 temperature=1，且 max_tokens > thinking.budget_tokens
        if thinking and thinking.get("type") == "enabled":
            payload["thinking"] = thinking
            payload["temperature"] = 1.0
            budget = int(thinking.get("budget_tokens") or 5000)
            if max_tokens <= budget:
                payload["max_tokens"] = budget + 4096
        else:
            payload["temperature"] = temperature

        # ★ system：自动包装成数组并打 cache_control（ttl=1h）
        if system:
            if isinstance(system, str):
                payload["system"] = [{
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral", "ttl": "1h"},
                }]
            else:
                # 前端已经传了数组结构（分静态/动态块），直接用
                payload["system"] = system

        headers = {
            "x-api-key": settings.UPSTREAM_API_KEY,
            "Authorization": f"Bearer {settings.UPSTREAM_API_KEY}",  # 兼容两种鉴权头
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

        # 累积 assistant 消息（用于回写到 convo）
        assistant_content: list[dict[str, Any]] = []
        # 正在构建的 content block 索引 → 内容
        building: dict[int, dict[str, Any]] = {}
        # 工具输入 JSON 累积器：index → partial_json string
        tool_json_buf: dict[int, str] = {}
        stop_reason: str | None = None
        # ★ usage 信息（包含缓存命中数据）
        usage_info: dict[str, Any] = {}

        async with httpx.AsyncClient(timeout=settings.UPSTREAM_TIMEOUT) as cli:
            async with cli.stream(
                "POST",
                f"{settings.UPSTREAM_API_BASE}/messages",
                headers=headers,
                json=payload,
            ) as resp:
                if resp.status_code >= 400:
                    body = await resp.aread()
                    err_text = body.decode("utf-8", errors="replace")
                    log.error(f"上游 {resp.status_code}: {err_text}")
                    yield _sse("error", {
                        "message": f"上游返回 {resp.status_code}",
                        "detail": err_text[:500],
                    })
                    return

                # 解析 SSE
                current_event: str | None = None
                async for line in resp.aiter_lines():
                    if not line:
                        current_event = None
                        continue
                    if line.startswith("event:"):
                        current_event = line[6:].strip()
                        continue
                    if not line.startswith("data:"):
                        continue
                    data_str = line[5:].strip()
                    if not data_str:
                        continue
                    try:
                        ev = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    etype = ev.get("type") or current_event

                    if etype == "message_start":
                        # ★ 首包带 usage 信息（含 cache_creation/read）
                        msg = ev.get("message", {})
                        u = msg.get("usage", {})
                        if u:
                            usage_info.update(u)

                    elif etype == "content_block_start":
                        idx = ev.get("index", 0)
                        block = ev.get("content_block", {})
                        building[idx] = dict(block)
                        if block.get("type") == "tool_use":
                            tool_json_buf[idx] = ""
                            yield _sse("tool_call_start", {
                                "id": block.get("id"),
                                "name": block.get("name"),
                                "index": idx,
                            })

                    elif etype == "content_block_delta":
                        idx = ev.get("index", 0)
                        delta = ev.get("delta", {})
                        dtype = delta.get("type")
                        if dtype == "text_delta":
                            text = delta.get("text", "")
                            if text:
                                yield _sse("content_delta", {"text": text})
                                # 累积到 building
                                blk = building.setdefault(idx, {"type": "text", "text": ""})
                                blk["text"] = blk.get("text", "") + text
                        elif dtype == "input_json_delta":
                            partial = delta.get("partial_json", "")
                            tool_json_buf[idx] = tool_json_buf.get(idx, "") + partial
                        elif dtype == "thinking_delta":
                            # 透传给前端，前端可选展示
                            yield _sse("thinking_delta", {"text": delta.get("thinking", "")})

                    elif etype == "content_block_stop":
                        idx = ev.get("index", 0)
                        blk = building.get(idx)
                        if blk and blk.get("type") == "tool_use":
                            raw = tool_json_buf.get(idx, "")
                            try:
                                blk["input"] = json.loads(raw) if raw else {}
                            except json.JSONDecodeError:
                                blk["input"] = {}
                            assistant_content.append(blk)
                        elif blk:
                            assistant_content.append(blk)

                    elif etype == "message_delta":
                        # 包含 stop_reason 和最终 usage
                        delta = ev.get("delta", {})
                        if delta.get("stop_reason"):
                            stop_reason = delta["stop_reason"]
                        u = ev.get("usage", {})
                        if u:
                            usage_info.update(u)

                    elif etype == "message_stop":
                        pass

        # ★ 打印 + 上报缓存命中情况
        cache_write = usage_info.get("cache_creation_input_tokens", 0)
        cache_read = usage_info.get("cache_read_input_tokens", 0)
        input_t = usage_info.get("input_tokens", 0)
        output_t = usage_info.get("output_tokens", 0)
        total_in = input_t + cache_write + cache_read
        hit_rate = round(cache_read / total_in * 100, 1) if total_in > 0 else 0
        if cache_read > 0:
            log.info(f"✅ 缓存命中 {cache_read} tok | write={cache_write} input={input_t} out={output_t} | 命中率 {hit_rate}%")
        elif cache_write > 0:
            log.info(f"📝 写入缓存 {cache_write} tok（下次会命中）| input={input_t} out={output_t}")
        else:
            log.info(f"❌ 缓存未生效 | input={input_t} out={output_t}")

        # 把 usage 也透传给前端（前端可选显示）
        yield _sse("usage", {
            "input_tokens": input_t,
            "output_tokens": output_t,
            "cache_creation_input_tokens": cache_write,
            "cache_read_input_tokens": cache_read,
            "hit_rate": hit_rate,
        })

        # 一轮 stream 完了
        yield _sse("turn_done", {"stop_reason": stop_reason or "unknown"})

        # 把这一轮的 assistant message 加进对话历史
        if assistant_content:
            convo.append({"role": "assistant", "content": assistant_content})

        # 看是否需要继续循环
        tool_uses = [b for b in assistant_content if b.get("type") == "tool_use"]
        if not tool_uses or stop_reason != "tool_use":
            log.info("无 tool_use 或 stop_reason 非 tool_use，循环结束")
            return

        # 并发执行所有工具
        async def _exec(tu: dict[str, Any]) -> dict[str, Any]:
            name = tu.get("name", "")
            args = tu.get("input", {}) or {}
            tid = tu.get("id", "")
            log.info(f"调用工具 {name} args={args}")
            try:
                result = await _dispatch_tool(name, args, routes, memory_client, notion)
                ok = not result.startswith("[error]") and not result.startswith("[notion error")
                preview = result if len(result) < 400 else result[:400] + "...(truncated)"
                return {
                    "tool_use_id": tid,
                    "name": name,
                    "ok": ok,
                    "result": result,
                    "preview": preview,
                }
            except Exception as e:
                log.exception(f"工具 {name} 失败")
                return {
                    "tool_use_id": tid,
                    "name": name,
                    "ok": False,
                    "result": f"[error] {type(e).__name__}: {e}",
                    "preview": f"[error] {e}",
                }

        # 先通知前端正在调哪些工具
        for tu in tool_uses:
            yield _sse("tool_call", {
                "id": tu.get("id"),
                "name": tu.get("name"),
                "input": tu.get("input"),
            })

        results = await asyncio.gather(*(_exec(tu) for tu in tool_uses))

        # 把结果通知前端
        for r in results:
            yield _sse("tool_result", {
                "id": r["tool_use_id"],
                "name": r["name"],
                "ok": r["ok"],
                "preview": r["preview"],
            })

        # 把 tool_result 喂回 convo，准备下一轮
        convo.append({
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": r["tool_use_id"],
                    "content": r["result"],
                    **({"is_error": True} if not r["ok"] else {}),
                }
                for r in results
            ],
        })

    log.warning(f"达到最大工具循环轮数 {settings.MAX_TOOL_ROUNDS}，强制终止")
    yield _sse("error", {"message": f"达到最大工具循环轮数 {settings.MAX_TOOL_ROUNDS}"})
