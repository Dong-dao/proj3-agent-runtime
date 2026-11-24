"""observer.py：/proc 式只读导出与控制面 —— S1 提供最小只读导出，控制面 S2 接管。

机制（设计文档 3.2.6）：导出任务图快照、各 Agent 状态与配额、句柄表、
上下文引用、指标当前值；控制指令 pause / resume / kill 走统一入口。
不变量：导出是只读快照，不改变调度状态；控制指令只作用于目标 Agent。

S1 范围：snapshot() 汇总 AgentTable / ResourceManager / ContextStore / Scheduler
的 stats()，供 bench 与 results.json 采集；pause / resume / kill 为 S2 交付。
"""

from __future__ import annotations

import logging

log = logging.getLogger("runtime.observer")


class Observer:
    """只读状态导出（/proc 同构视图，不挂载真实 procfs）。"""

    def __init__(self, table, resources, context_store=None, scheduler=None) -> None:
        self._table = table
        self._resources = resources
        self._context = context_store
        self._scheduler = scheduler

    def snapshot(self) -> dict:
        """只读快照：导出不改状态（P5 可观测即接口）。"""
        agents = {
            agent_id: {
                "state": agent.state.name,
                "quota": agent.quota,
                "handles": sorted(agent.handles),
                "ctx_refs": sorted(agent.ctx_refs),
            }
            for agent_id, agent in self._table.all_agents().items()
        }
        return {
            "agents": agents,
            "agent_counters": self._table.stats(),
            "resource_counters": self._resources.stats(),
            "context_stats": self._context.stats() if self._context else None,
            "scheduler_stats": self._scheduler.stats() if self._scheduler else None,
        }

    def pause(self, agent_id: str) -> None:
        """S2 交付：控制指令，走统一入口并记日志。"""
        log.warning("pause(%s): S2 交付，S1 未实现", agent_id)
        raise NotImplementedError("pause 在 S2 实现")

    def resume(self, agent_id: str) -> None:
        """S2 交付：控制指令，走统一入口并记日志。"""
        log.warning("resume(%s): S2 交付，S1 未实现", agent_id)
        raise NotImplementedError("resume 在 S2 实现")

    def kill(self, agent_id: str) -> None:
        """S2 交付：控制指令，走统一入口并记日志。"""
        log.warning("kill(%s): S2 交付，S1 未实现", agent_id)
        raise NotImplementedError("kill 在 S2 实现")
