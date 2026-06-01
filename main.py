"""
Lily-Celyn 后端主入口
- 代理 msuicode /v1/messages（Anthropic 原生格式，流式）
- 工具循环：合并 Celyn's Memory MCP + Notion 工具集
- SSE 流式输出给前端
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from config import settings
from tool_loop import run_tool_loop
from mcp_client import CelynMemoryClient
from notion_tools import NotionTools

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("lily")


# ============ 启动 / 关闭钩子 ============

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Lily 后端启动中...")
    # 启动时预热一次工具列表（缓存）
    app.state.memory_client = CelynMemoryClient(
        url=settings.CELYN_MEMORY_MCP_URL,
        bearer=settings.CELYN_MEMORY_BEARER,
    )
    app.state.notion = NotionTools(token=settings.NOTION_TOKEN)
    try:
        memory_tools = await app.state.memory_client.list_tools()
        log.info(f"Celyn's Memory 已连通，共 {len(memory_tools)} 个工具")
    except Exception as e:
        log.warning(f"Celyn's Memory 预热失败（不影响启动）: {e}")

    notion_tools = app.state.notion.tool_schemas()
    log.info(f"Notion 工具集就绪，共 {len(notion_tools)} 个工具")

    yield
    log.info("Lily 后端关闭")


app = FastAPI(title="Lily-Celyn Backend", lifespan=lifespan)

# ============ CORS ============

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ 健康检查 ============

@app.get("/")
async def root():
    return {"name": "Lily-Celyn Backend", "status": "alive"}


@app.get("/api/health")
async def health():
    return {"ok": True}


# ============ 列工具（前端可展示有哪些能力） ============

@app.get("/api/tools")
async def list_all_tools(request: Request):
    memory_client: CelynMemoryClient = request.app.state.memory_client
    notion: NotionTools = request.app.state.notion

    memory_tools: list[dict[str, Any]] = []
    try:
        memory_tools = await memory_client.list_tools()
    except Exception as e:
        log.warning(f"读取 Memory 工具失败：{e}")

    return {
        "memory": [t["name"] for t in memory_tools],
        "notion": [t["name"] for t in notion.tool_schemas()],
    }


# ============ 聊天主入口（流式） ============

@app.post("/api/chat")
async def chat(request: Request):
    """
    入参（JSON）：
    {
        "model": "claude-opus-4-7",
        "messages": [{"role":"user","content":"..."}],
        "system": "可选系统提示",
        "max_tokens": 8192,
        "temperature": 1.0
    }
    出参：SSE 流。事件类型：
    - event: content_delta      data: {"text":"..."}        模型文本增量
    - event: tool_call          data: {"name":"...", "input":{...}, "id":"..."}  即将调用工具
    - event: tool_result        data: {"id":"...", "ok":true, "preview":"..."}   工具完成
    - event: turn_done          data: {"stop_reason":"..."}  本轮结束
    - event: done               data: {}                     整次对话结束
    - event: error              data: {"message":"..."}      出错
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON body")

    model: str = body.get("model") or "claude-opus-4-7"
    messages: list[dict[str, Any]] = body.get("messages") or []
    system: str | None = body.get("system")
    max_tokens: int = int(body.get("max_tokens") or 8192)
    temperature: float = float(body.get("temperature") if body.get("temperature") is not None else 1.0)

    if not messages:
        raise HTTPException(status_code=400, detail="messages is required")

    memory_client: CelynMemoryClient = request.app.state.memory_client
    notion: NotionTools = request.app.state.notion

    async def event_stream():
        try:
            async for evt in run_tool_loop(
                model=model,
                messages=messages,
                system=system,
                max_tokens=max_tokens,
                temperature=temperature,
                memory_client=memory_client,
                notion=notion,
            ):
                yield evt
        except Exception as e:
            log.exception("chat stream failed")
            err = json.dumps({"message": str(e)}, ensure_ascii=False)
            yield f"event: error\ndata: {err}\n\n"
        finally:
            yield "event: done\ndata: {}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ============ 全局错误处理 ============

@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    log.exception("unhandled exception")
    return JSONResponse(
        status_code=500,
        content={"error": "internal_error", "detail": str(exc)},
    )
