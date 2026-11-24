"""统一 Resource Handle：字段、错误码（契约① 第 3 节，2026-11-17 冻结）。

不变量（维护）：
- 句柄不可伪造：handle_id 由 ResourceManager 统一发放，查询不到即 E_BAD_HANDLE；
- 越权必拒：非 owner 操作返回 E_PERM；
- 超额必拒：used + amount > quota 返回 E_QUOTA；
- 已关闭：对 CLOSED 句柄操作返回 E_CLOSED；
- 实体进入终态时由 AgentTable 触发 release_all() 全部回收。

日志（产生）：
- 每次 acquire / use / release / release_all 记一条 INFO 日志（runtime.resource）；
- 每次拒绝记一条 WARNING 日志，携带错误码。

错误码集合固定为：E_ILLEGAL_TRANSITION / E_PERM / E_QUOTA / E_BAD_HANDLE / E_CLOSED。
DAG 环检测另用 E_DAG_CYCLE（日志与计数用，不占用上述公开错误码）。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from enum import Enum

log = logging.getLogger("runtime.resource")

# ---- 错误码（契约① 固定集合） -------------------------------------------
E_ILLEGAL_TRANSITION = "E_ILLEGAL_TRANSITION"
E_PERM = "E_PERM"
E_QUOTA = "E_QUOTA"
E_BAD_HANDLE = "E_BAD_HANDLE"
E_CLOSED = "E_CLOSED"
E_DAG_CYCLE = "E_DAG_CYCLE"  # 日志与计数用，不占用公开错误码

# 终态
STATE_OPEN = "OPEN"
STATE_CLOSED = "CLOSED"


class RuntimeErr(Exception):
    """Runtime 统一错误：code 取上述错误码之一。"""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


class HandleKind(Enum):
    """资源种类：模型 / 工具 / 文件 / 内存配额（契约① 第 3 节）。"""

    LLM = 1
    TOOL = 2
    FILE = 3
    MEMORY = 4


@dataclass(frozen=True)
class ResourceHandle:
    """统一句柄：模型、工具、文件、内存配额长得不一样，但申请方式一样（P3）。

    frozen=True：字段变更一律经 dataclasses.replace 生成新对象，
    保证句柄对象在并发读取时不会被半写。
    """

    handle_id: str            # Runtime 统一发放，不可伪造
    kind: HandleKind
    owner: str                # agent_id，随实体终止而回收
    perms: frozenset[str]     # READ / WRITE / EXEC
    quota: int
    used: int
    refs: int                 # 引用计数
    state: str                # OPEN / CLOSED
