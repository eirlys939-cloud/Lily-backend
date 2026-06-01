"""
环境变量配置——所有密钥从环境读取，不进代码。
"""
import os
from dataclasses import dataclass, field


def _split_origins(raw: str) -> list[str]:
    items = [x.strip() for x in raw.split(",") if x.strip()]
    return items or ["*"]


@dataclass
class Settings:
    # 上游 Claude API（msuicode 中转）
    UPSTREAM_API_BASE: str = os.getenv("UPSTREAM_API_BASE", "https://www.msuicode.com/v1")
    UPSTREAM_API_KEY: str = os.getenv("UPSTREAM_API_KEY", "")

    # Celyn's Memory MCP
    CELYN_MEMORY_MCP_URL: str = os.getenv("CELYN_MEMORY_MCP_URL", "https://celyn-brain.zeabur.app/mcp")
    CELYN_MEMORY_BEARER: str = os.getenv("CELYN_MEMORY_BEARER", "")

    # Notion Internal Integration Token（ntn_ 开头）
    NOTION_TOKEN: str = os.getenv("NOTION_TOKEN", "")
    NOTION_VERSION: str = os.getenv("NOTION_VERSION", "2022-06-28")

    # CORS：逗号分隔的允许来源；开发期可以填 *
    ALLOWED_ORIGINS: list[str] = field(
        default_factory=lambda: _split_origins(os.getenv("ALLOWED_ORIGINS", "*"))
    )

    # 工具循环安全上限（防止无限递归）
    MAX_TOOL_ROUNDS: int = int(os.getenv("MAX_TOOL_ROUNDS", "20"))

    # 上游请求超时（秒）
    UPSTREAM_TIMEOUT: float = float(os.getenv("UPSTREAM_TIMEOUT", "300"))


settings = Settings()


def assert_ready():
    """启动前检查关键密钥是否存在，缺哪个吼哪个。"""
    missing = []
    if not settings.UPSTREAM_API_KEY:
        missing.append("UPSTREAM_API_KEY")
    if not settings.CELYN_MEMORY_BEARER:
        missing.append("CELYN_MEMORY_BEARER")
    if not settings.NOTION_TOKEN:
        missing.append("NOTION_TOKEN")
    if missing:
        raise RuntimeError(f"缺少环境变量: {', '.join(missing)}")
