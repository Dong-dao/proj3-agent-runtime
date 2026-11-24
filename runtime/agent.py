"""agent.py：六态状态机与 Agent Table（契约① 第 1 节 + 调度器接口的 transition()）。

对应机制：P2 一等公民 + 单一状态入口（OS 出处：PCB 与进程状态机）。

不变量（维护）：
- I1 状态唯一：任一时刻一个 Agent 只有一个 state 值，所有变更必经 transition()；
- I2 终态不可回退：TERMINATED / FAILED 行全 ✗，终态自环也禁止；
- I3 阻塞只回就绪：BLOCKED 出边只有 READY 与 FAILED；
- I4 终态回收：进入终态时 release_all() 回收全部句柄并清空上下文引用。

日志（产生）：
- 每次合法转换记一条 INFO 日志（runtime.agent）；
- 每次非法转换记一条 WARNING 日志并抛 E_ILLEGAL_TRANSITION；
- 终态回收记一条 INFO 日志，携带回收句柄数。

计数：迁移条数 transitions、非法拦截次数 illegal_intercepts、终态回收句柄数
reclaimed_handles（经 AgentTable.stats() 导出，供 Observer / results.json 采集）。
"""

from __future__ import annotations

import logging
import threading
from enum import Enum

from .handle import (
    E_ILLEGAL_TRANSITION,
    STATE_CLOSED,
    RuntimeErr,
)

log = logging.getLogger("runtime.agent")


class AgentState(Enum):
    """六态（契约① 第 1 节，语义与 docs/design/六态状态机与合法转换矩阵.md 一致）。"""

    CREATED = 1       # 已登记，依赖未满足，尚未入队
    READY = 2         # 依赖满足，在就绪队列中等待派发
    RUNNING = 3       # 已被派发，占用 worker 执行 stub
    BLOCKED = 4       # 等待 IO / 句柄 / 子任务，可回到 READY
    TERMINATED = 5    # 正常完成，终态
    FAILED = 6        # 超时 / 异常 / 配额拒绝 / 熔断，终态


# 合法转换矩阵（6x6 的压缩表示：from -> 允许的 to 集合，共 11 条允许边）。
# 与 docs/design/六态状态机与合法转换矩阵.md 第二节逐格一致；终态行全空。
LEGAL_TRANSITIONS: dict[AgentState, frozenset[AgentState]] = {
    AgentState.CREATED: frozenset({AgentState.READY, AgentState.FAILED}),
    AgentState.READY: frozenset({AgentState.READY, AgentState.RUNNING, AgentState.FAILED}),
    AgentState.RUNNING: frozenset(
        {AgentState.BLOCKED, AgentState.TERMINATED, AgentState.FAILED}
    ),
    AgentState.BLOCKED: frozenset({AgentState.READY, AgentState.FAILED}),
    AgentState.TERMINATED: frozenset(),
    AgentState.FAILED: frozenset(),
}


class Agent:
    """Agent 实体：有标识、有状态、有配额、有句柄集、有上下文引用（一等公民）。"""

    def __init__(self, agent_id: str, quota: int = 0) -> None:
        self.agent_id = agent_id
        self.state = AgentState.CREATED
        self.quota = quota
        self.handles: set[str] = set()      # 持有的 handle_id 集合
        self.ctx_refs: set[str] = set()     # 持有的上下文引用集合

    def transition(self, to: AgentState, table: "AgentTable") -> None:
        """经 AgentTable 的单一入口转换状态；禁止在模块内直接给 state 赋值。"""
        table.transition(self.agent_id, to)


class AgentTable:
    """Agent Table：调度器事件循环（单一写入者）独占其写权限。

    resource_manager 由组装方注入（bind_resources），避免 agent -> resource
    的模块级反向依赖；终态回收经由注入对象的 release_all() 完成。
    """

    def __init__(self) -> None:
        self._agents: dict[str, Agent] = {}
        self._lock = threading.Lock()  # 只保护注册表写入，不改变单一写入者语义
        self._resource_manager = None  # 注入的 ResourceManager
        self._context_store = None     # 注入的 ContextStore
        self._counters = {
            "transitions": 0,
            "illegal_intercepts": 0,
            "reclaimed_handles": 0,
        }

    # ---- 组装 ------------------------------------------------------------
    def bind_resources(self, resource_manager, context_store=None) -> None:
        """注入句柄回收依赖（终态回收 I4 需要）。"""
        self._resource_manager = resource_manager
        self._context_store = context_store

    # ---- 注册与查询 --------------------------------------------------------
    def create(self, agent_id: str, quota: int = 0) -> Agent:
        """登记新 Agent，初始状态 CREATED。

        维护：agent_id 全局唯一；产生：注册 DEBUG 日志。
        """
        with self._lock:
            if agent_id in self._agents:
                raise RuntimeErr(E_ILLEGAL_TRANSITION, f"agent {agent_id} 已存在，拒绝重复登记")
            agent = Agent(agent_id, quota)
            self._agents[agent_id] = agent
        log.debug("agent registered: %s (CREATED)", agent_id)
        return agent

    def get(self, agent_id: str) -> Agent:
        """按 id 查询 Agent；不存在抛 E_BAD_HANDLE（id 由 Runtime 发放，不可伪造）。"""
        agent = self._agents.get(agent_id)
        if agent is None:
            raise RuntimeErr("E_BAD_HANDLE", f"agent {agent_id} 不存在")
        return agent

    def all_agents(self) -> dict[str, Agent]:
        """只读视图：返回 {agent_id: Agent} 快照副本（导出不改状态）。"""
        return dict(self._agents)

    # ---- 单一状态入口 ------------------------------------------------------
    def transition(self, agent_id: str, to: AgentState) -> None:
        """唯一状态入口（契约①）：校验合法转换矩阵。

        维护：I1/I2/I3 状态不变量 + I4 终态回收；
        产生：合法转换 INFO 日志、非法转换 WARNING 日志（携带 E_ILLEGAL_TRANSITION）。
        """
        agent = self.get(agent_id)
        # 检查 + 写入必须原子（I1 状态唯一）：防止并发下两个转换都通过校验
        with self._lock:
            current = agent.state
            if to not in LEGAL_TRANSITIONS[current]:
                self._counters["illegal_intercepts"] += 1
                log.warning(
                    "E_ILLEGAL_TRANSITION %s: %s -> %s 被拒绝（矩阵不允许）",
                    agent_id, current.name, to.name,
                )
                raise RuntimeErr(
                    E_ILLEGAL_TRANSITION,
                    f"{agent_id}: {current.name} -> {to.name} 不在合法转换矩阵内",
                )
            agent.state = to
            self._counters["transitions"] += 1
        log.info("transition %s: %s -> %s", agent_id, current.name, to.name)
        if to in (AgentState.TERMINATED, AgentState.FAILED):
            self._reclaim_on_terminal(agent)

    def _reclaim_on_terminal(self, agent: Agent) -> None:
        """I4：进入终态时回收全部句柄与上下文引用。"""
        if self._resource_manager is not None and agent.handles:
            reclaimed = self._resource_manager.release_all(agent.agent_id)
            self._counters["reclaimed_handles"] += reclaimed
            agent.handles.clear()
            log.info("terminal reclaim %s: 回收句柄 %d 个", agent.agent_id, reclaimed)
        if self._context_store is not None and agent.ctx_refs:
            for ctx_id in list(agent.ctx_refs):
                self._context_store.release(agent.agent_id, ctx_id)
            agent.ctx_refs.clear()

    # ---- 计数 --------------------------------------------------------------
    def stats(self) -> dict:
        """计数导出：迁移条数 / 非法拦截次数 / 终态回收句柄数。"""
        return dict(self._counters)


# 模块级默认表：transition() 全局函数的载体；多实例场景可自行建表传入。
_DEFAULT_TABLE = AgentTable()


def transition(agent_id: str, to: AgentState, table: AgentTable | None = None) -> None:
    """全局唯一状态入口（契约① 第 5 节签名）。

    非法转换抛 RuntimeErr(E_ILLEGAL_TRANSITION) 并记一条拒绝日志。
    """
    (_DEFAULT_TABLE if table is None else table).transition(agent_id, to)
