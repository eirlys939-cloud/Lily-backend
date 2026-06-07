"""
工具循环：tool_use ↔ tool_result 多轮，流式 SSE 输出。

工作流：
1. 把 messages + tools 发给上游（msuicode /v1/messages，stream=True）
2. 流式解析事件，把文本增量实时 yield 给前端
3. 遇到 tool_use block：完整收集 → 并发执行（Memory MCP / Notion）→ 喂回 messages
4. 如果 stop_reason == "tool_use"，回到步骤 1 再来一轮；否则结束

【Prompt Caching 说明】
- system prompt：自动包装成数组结构并打 cache_control（前端传字符串即可）
- tools 数组：在最后一个工具上打 cache_control（整个工具列表会被缓存)
- metadata.user_id：固定为 "lily-celyn"，保证请求路由到同一台服务器

【BP3 滚动压缩说明】
- 触发条件：messages 数 >= BP3_COMPRESS_THRESHOLD（默认 60）
- 压缩策略：保留最近 BP3_KEEP_RECENT 条（默认 30），其余调 Haiku 总结
- 摘要模型：claude-haiku-4-5-20251001
- 摘要去向：① 替换 messages 数组中的旧消息（一对 user/assistant）
            ② fire-and-forget 调 Celyn's Memory hold 永久存档
- 缓存影响：压缩点后 BP4 锚点重新计算，会触发一次 cache_write，下次开始稳定

【A 方案：摘要复用缓存】
- 问题：前端不知道后端已压缩，每次发全量历史，后端每次都重新调 Haiku 浪费钱
- 解法：进程内 TTL 缓存，key=待压缩段的 SHA256 指纹，value=摘要文本
- TTL：24 小时；容量：100 条（LRU 淘汰）
- 重启即失效：Zeabur 重启后第一次会重新调 Haiku，之后稳定复用
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from typing import Any, AsyncGenerator

import httpx

from config import settings
from mcp_client import CelynMemoryClient
from notion_tools import NotionTools

log = logging.getLogger("lily.loop")


# ============ BP3 滚动压缩配置 ============

BP3_COMPRESS_THRESHOLD = 60       # messages 数 >= 这个值就触发压缩
BP3_KEEP_RECENT = 30              # 压缩时保留最近多少条
BP3_SUMMARY_MODEL = "claude-haiku-4-5-20251001"
BP3_SUMMARY_MAX_TOKENS = 2000     # 摘要本身的 max_tokens

# A 方案：摘要复用缓存（进程内 TTL + LRU）
BP3_CACHE_TTL = 86400             # 摘要缓存有效期（秒），24 小时
BP3_CACHE_MAX = 100               # 缓存条目上限，超过按 LRU 淘汰最老
_BP3_SUMMARY_CACHE: dict[str, tuple[float, str]] = {}  # key -> (存入时间戳, 摘要文本)
_BP3_CACHE_HITS = 0               # 累计命中次数（仅用于日志）
_BP3_CACHE_MISSES = 0             # 累计未命中次数

BP3_SUMMARY_PROMPT = """你是一个对话压缩助手。下面是 Lily 和 Celyn 之间一段较长的对话历史。
请把它浓缩成一段紧凑的摘要文本，供 Celyn 后续对话时回顾上下文。

输出要求（严格按结构）：
【事实】列出对话中提到的具体事实、决定、技术细节、人物事件，按时间顺序，每条一行。
【情绪】Lily 在这段对话里的情绪变化与状态，简明扼要。
【待办】对话中明确或隐含的下一步行动、未完成的事项。

风格：
- 客观、简练、信息密度高，不要寒暄不要修辞。
- 保留所有人名、技术名词、数字、时间戳。
- 不要用 markdown，纯文本即可。
- 总长度控制在 1500 token 以内。

对话历史如下："""


def _sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _fingerprint_messages(msgs: list[dict[str, Any]]) -> str:
    """
    给一段 messages 计算 SHA256 指纹。
    用于 A 方案摘要缓存的 key：相同内容 -> 相同指纹 -> 复用摘要。
    """
    h = hashlib.sha256()
    for m in msgs:
        role = m.get("role", "?")
        content = m.get("content")
        h.update(role.encode("utf-8"))
        h.update(b"\x00")
        # content 可能是 str 或 list[dict]，统一序列化
        if isinstance(content, str):
            h.update(content.encode("utf-8"))
        else:
            h.update(json.dumps(content, ensure_ascii=False, sort_keys=True).encode("utf-8"))
        h.update(b"\x01")
    return h.hexdigest()


def _cache_get(key: str) -> str | None:
    """查摘要缓存。命中且未过期 -> 返回摘要；否则返回 None。"""
    global _BP3_CACHE_HITS, _BP3_CACHE_MISSES
    entry = _BP3_SUMMARY_CACHE.get(key)
    if entry is None:
        _BP3_CACHE_MISSES += 1
        return None
    ts, summary = entry
    if time.time() - ts > BP3_CACHE_TTL:
        # 过期了，删除并视为未命中
        _BP3_SUMMARY_CACHE.pop(key, None)
        _BP3_CACHE_MISSES += 1
        return None
    # 命中：更新时间戳实现 LRU "刷新"
    _BP3_SUMMARY_CACHE[key] = (time.time(), summary)
    _BP3_CACHE_HITS += 1
    return summary


def _cache_set(key: str, summary: str) -> None:
    """写摘要缓存。超过容量上限时淘汰最老的条目。"""
    if len(_BP3_SUMMARY_CACHE) >= BP3_CACHE_MAX and key not in _BP3_SUMMARY_CACHE:
        # 找最老的一条删掉（按时间戳）
        oldest_key = min(_BP3_SUMMARY_CACHE.items(), key=lambda kv: kv[1][0])[0]
        _BP3_SUMMARY_CACHE.pop(oldest_key, None)
    _BP3_SUMMARY_CACHE[key] = (time.time(), summary)


def _format_messages_for_summary(msgs: list[dict[str, Any]]) -> str:
    """把 messages 数组转成纯文本喂给 Haiku 做摘要。"""
    lines: list[str] = []
    for m in msgs:
        role = m.get("role", "?")
        content = m.get("content")
        if isinstance(content, str):
            lines.append(f"[{role}] {content}")
        elif isinstance(content, list):
            parts: list[str] = []
            for blk in content:
                btype = blk.get("type")
                if btype == "text":
                    parts.append(blk.get("text", ""))
                elif btype == "tool_use":
                    parts.append(f"<调用工具 {blk.get('name')} 参数={json.dumps(blk.get('input', {}), ensure_ascii=False)}>")
                elif btype == "tool_result":
                    raw = blk.get("content", "")
                    if isinstance(raw, list):
                        raw = "".join(b.get("text", "") for b in raw if isinstance(b, dict))
                    snippet = str(raw)[:300]
                    parts.append(f"<工具返回 {snippet}>")
                elif btype == "thinking":
                    pass  # thinking 不进摘要
            joined = "\n".join(p for p in parts if p)
            if joined:
                lines.append(f"[{role}] {joined}")
    return "\n\n".join(lines)


async def _summarize_old_messages(old_msgs: list[dict[str, Any]]) -> str | None:
    """调 Haiku 把旧消息压缩成摘要文本。失败返回 None。"""
    if not old_msgs:
        return None
    convo_text = _format_messages_for_summary(old_msgs)
    if not convo_text.strip():
        return None

    payload = {
        "model": BP3_SUMMARY_MODEL,
        "max_tokens": BP3_SUMMARY_MAX_TOKENS,
        "messages": [{
            "role": "user",
            "content": BP3_SUMMARY_PROMPT + "\n\n" + convo_text,
        }],
        "temperature": 0.3,  # 摘要要稳定，温度调低
    }
    headers = {
        "x-api-key": settings.UPSTREAM_API_KEY,
        "Authorization": f"Bearer {settings.UPSTREAM_API_KEY}",
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as cli:
            r = await cli.post(
                f"{settings.UPSTREAM_API_BASE}/messages",
                headers=headers,
                json=payload,
            )
            if r.status_code >= 400:
                log.error(f"BP3 摘要失败：上游 {r.status_code} {r.text[:300]}")
                return None
            data = r.json()
            blocks = data.get("content", [])
            text_parts = [b.get("text", "") for b in blocks if b.get("type") == "text"]
            summary = "\n".join(text_parts).strip()
            if not summary:
                return None
            log.info(f"BP3 摘要生成成功：{len(summary)} 字符")
            return summary
    except Exception as e:
        log.exception(f"BP3 摘要异常：{e}")
        return None


async def _save_summary_to_memory(
    summary: str,
    msg_count: int,
    memory_client: CelynMemoryClient,
) -> None:
    """fire-and-forget：把摘要顺手写进 Celyn's Memory。失败不抛错。"""
    try:
        content = f"对话历史压缩摘要（{msg_count} 条消息）：\n\n{summary}"
        await memory_client.call_tool("hold", {
            "content": content,
            "tags": "对话压缩,自动归档,BP3",
            "importance": 7,
            "pinned": False,
        })
        log.info("BP3 摘要已存入 Celyn's Memory")
    except Exception as e:
        log.warning(f"BP3 摘要存入 Memory 失败（不影响主流程）：{e}")


async def _compress_if_needed(
    convo: list[dict[str, Any]],
    memory_client: CelynMemoryClient,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """
    BP3 压缩入口。返回 (新的 convo, 压缩信息字典 or None)。

    压缩信息字典格式：
        {
            "triggered": True,
            "original_count": 50,
            "compressed_count": 21,  # 1 条摘要伪 user + 20 条保留
            "summary_chars": 1234,
        }
    """
    if len(convo) < BP3_COMPRESS_THRESHOLD:
        return convo, None

    # 切分：前面要压缩的 + 后面要保留的
    keep_start = len(convo) - BP3_KEEP_RECENT
    old_part = convo[:keep_start]
    recent_part = convo[keep_start:]

    # ★ 重要：recent_part 第一条必须是 user，否则 Anthropic 会报错
    # 因为我们要在前面塞一对 user/assistant，最后一条 assistant 之后必须接 user
    # 如果 recent_part[0] 不是 user，往前再多收一条
    while recent_part and recent_part[0].get("role") != "user":
        # 把 recent_part[0] 让回 old_part
        old_part.append(recent_part.pop(0))

    if not old_part or not recent_part:
        return convo, None

    log.info(f"🗜️  BP3 触发压缩：旧={len(old_part)} 条，保留={len(recent_part)} 条")

    # ★ A 方案：先查缓存
    fingerprint = _fingerprint_messages(old_part)
    cached = _cache_get(fingerprint)
    if cached is not None:
        summary = cached
        cache_hit = True
        log.info(
            f"♻️  BP3 摘要缓存命中（key={fingerprint[:12]}…，"
            f"hits={_BP3_CACHE_HITS}/misses={_BP3_CACHE_MISSES}），跳过 Haiku"
        )
    else:
        summary = await _summarize_old_messages(old_part)
        if not summary:
            log.warning("BP3 摘要失败，跳过压缩，按原样继续")
            return convo, None
        _cache_set(fingerprint, summary)
        cache_hit = False
        log.info(
            f"💾 BP3 摘要已写入缓存（key={fingerprint[:12]}…，"
            f"cache_size={len(_BP3_SUMMARY_CACHE)}）"
        )

    # 构造摘要伪消息对
    fake_pair = [
        {
            "role": "user",
            "content": f"[以下是我们之前对话的压缩摘要，请记住这些上下文：]\n\n{summary}",
        },
        {
            "role": "assistant",
            "content": "好的，我已经记住了这些上下文。我们继续。",
        },
    ]

    new_convo = fake_pair + recent_part

    # fire-and-forget 存进记忆库（仅在首次生成时存，缓存命中不重复存）
    if not cache_hit:
        asyncio.create_task(_save_summary_to_memory(summary, len(old_part), memory_client))

    info = {
        "triggered": True,
        "original_count": len(convo),
        "compressed_count": len(new_convo),
        "summary_chars": len(summary),
        "cache_hit": cache_hit,
    }
    return new_convo, info


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

    # ★ BP3 滚动压缩：进入循环前先检查是否需要压缩历史
    convo_initial, compress_info = await _compress_if_needed(list(messages), memory_client)
    if compress_info:
        cache_tag = "♻️ 复用缓存" if compress_info.get("cache_hit") else "🆕 新摘要"
        log.info(
            f"✂️  BP3 已压缩：{compress_info['original_count']} → "
            f"{compress_info['compressed_count']} 条消息，"
            f"摘要 {compress_info['summary_chars']} 字符 [{cache_tag}]"
        )
        yield _sse("bp3_compressed", compress_info)

    # 复制 messages，准备 in-place 追加 assistant / tool_result
    convo: list[dict[str, Any]] = convo_initial

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
