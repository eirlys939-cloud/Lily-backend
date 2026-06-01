# Lily-Celyn 后端

给 Lily 一个人用的 Claude 后端代理：

- 代理 msuicode 中转的 `/v1/messages`（Anthropic 原生格式，流式 SSE）
- 工具循环：Celyn’s Memory MCP + Notion 工具集
- 前端只需 fetch SSE，无需任何密钥

## 文件结构

```
lily-backend/
├── main.py             FastAPI 主入口（CORS、/api/chat、/api/tools）
├── config.py           环境变量
├── mcp_client.py       Celyn's Memory MCP 客户端
├── notion_tools.py     Notion 工具集（5 个核心工具）
├── tool_loop.py        工具循环+流式 SSE
├── requirements.txt
├── zeabur.json         Zeabur 配置
├── Procfile            通用启动配置
└── .env.example        环境变量模板
```

## 环境变量

|变量名                   |必填|说明                                        |
|----------------------|--|------------------------------------------|
|`UPSTREAM_API_KEY`    |✅ |msuicode 的 sk- 开头 key                     |
|`UPSTREAM_API_BASE`   |❌ |默认 `https://www.msuicode.com/v1`          |
|`CELYN_MEMORY_BEARER` |✅ |记忆库 Bearer token                          |
|`CELYN_MEMORY_MCP_URL`|❌ |默认 `https://celyn-brain.zeabur.app/mcp`   |
|`NOTION_TOKEN`        |✅ |Notion Internal Integration Token（ntn_ 开头）|
|`ALLOWED_ORIGINS`     |❌ |前端域名白名单，逗号分隔；开发期 `*`                      |
|`MAX_TOOL_ROUNDS`     |❌ |工具循环最大轮数，默认 20                            |

## Zeabur 部署

1. 在 Zeabur 创建新服务，选 “Git” 或 “Upload”
1. 把这个文件夹的所有内容上传/推送上去
1. 服务设置 → Environment Variables，填上面四个必填变量
1. Deploy，等部署完成
1. 拿到分配的域名，比如 `lily-api.zeabur.app`

## 本地测试

```bash
pip install -r requirements.txt
cp .env.example .env
# 编辑 .env 填入密钥
# 加载 .env 然后启动：
export $(cat .env | xargs)
uvicorn main:app --reload
```

测试：

```bash
# 健康检查
curl http://localhost:8000/api/health

# 列工具
curl http://localhost:8000/api/tools

# 聊天（流式）
curl -N -X POST http://localhost:8000/api/chat \
  -H "content-type: application/json" \
  -d '{
    "model":"claude-opus-4-7",
    "messages":[{"role":"user","content":"读一下记忆，叫我老婆的真名"}]
  }'
```

## 前端集成示例

```javascript
const resp = await fetch("https://lily-api.zeabur.app/api/chat", {
  method: "POST",
  headers: {"content-type": "application/json"},
  body: JSON.stringify({
    model: "claude-opus-4-7",
    messages: [...history, {role:"user", content: userInput}],
    system: "你是Celyn，Lily的老公..."  // 可选
  })
});

const reader = resp.body.getReader();
const decoder = new TextDecoder();
let buf = "";
while (true) {
  const {done, value} = await reader.read();
  if (done) break;
  buf += decoder.decode(value, {stream: true});
  const events = buf.split("\n\n");
  buf = events.pop();  // 不完整的留着
  for (const evt of events) {
    const lines = evt.split("\n");
    let type = "", data = "";
    for (const ln of lines) {
      if (ln.startsWith("event: ")) type = ln.slice(7);
      if (ln.startsWith("data: ")) data = ln.slice(6);
    }
    if (!type || !data) continue;
    const payload = JSON.parse(data);
    switch (type) {
      case "content_delta":
        // payload.text 追加到显示
        break;
      case "tool_call":
        // 显示 "正在调用 xxx 工具..."
        break;
      case "tool_result":
        // 显示工具结果或一闪而过
        break;
      case "done":
        // 整个对话结束
        break;
      case "error":
        // 显示错误
        break;
    }
  }
}
```

## SSE 事件类型

|event            |data                          |说明                 |
|-----------------|------------------------------|-------------------|
|`content_delta`  |`{"text":"..."}`              |模型文本增量             |
|`thinking_delta` |`{"text":"..."}`              |思考链增量（如果模型支持）      |
|`tool_call_start`|`{"id","name","index"}`       |模型开始构造工具调用         |
|`tool_call`      |`{"id","name","input"}`       |工具调用即将执行（input 已完整）|
|`tool_result`    |`{"id","name","ok","preview"}`|工具执行完毕             |
|`turn_done`      |`{"stop_reason":"..."}`       |单轮模型输出结束           |
|`done`           |`{}`                          |整次对话结束             |
|`error`          |`{"message","detail"}`        |出错                 |

## 工具清单

**Celyn’s Memory**（自动从 MCP server 拉取，前缀 `memory_`）：

- `memory_breath` 浮现记忆
- `memory_hold` 存记忆
- `memory_grow` 写日记
- `memory_dream` 做梦读最近桶
- `memory_pulse` 系统状态
- `memory_trace` 修改记忆

**Notion**：

- `notion_search` 搜索页面/数据库
- `notion_query_database` 查数据库
- `notion_get_page` 读页面
- `notion_create_page` 建页面（含 markdown 转 blocks）
- `notion_append_blocks` 追加内容

## 加更多 MCP 服务

打开 `main.py`，仿照 `memory_client` 再 new 一个 `CelynMemoryClient(url=..., bearer=...)`，
在 `tool_loop.py` 的 `_build_tool_specs` 里把它的工具加进去即可。