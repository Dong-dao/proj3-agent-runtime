"""fault.py：故障域与超时熔断 —— S1 仅骨架，S2 实现（本周规划红线：本周不做故障注入）。

S2 要实现的机制（设计文档 3.2.5）：
- 边界：调度器、消息总线、Observer 在故障域外；每个 Agent 的执行体在域内；
- 超时熔断 -> 任务置 FAILED -> 依赖它的任务按策略降级或重新排队 -> 其余任务继续；
- 不变量：故障不跨执行域；调度器在任何单体失败后仍可推进。

S1 已就位的对接点：Scheduler.on_fail(task_id, reason) 走 RUNNING -> FAILED
单一入口；本文件 S2 将提供 FaultDomain 与注入开关，计数恢复时间与受影响任务数。
"""

from __future__ import annotations

import logging

log = logging.getLogger("runtime.fault")


class FaultDomain:
    """故障域骨架：S2 实现超时熔断与注入开关（S1 不实例化）。"""

    def __init__(self, domain_id: str = "default") -> None:
        self.domain_id = domain_id
        self._members: set[str] = set()

    def register(self, agent_id: str) -> None:
        """登记域内成员（S1 只记账，不熔断）。"""
        self._members.add(agent_id)
        log.debug("fault domain %s: registered %s", self.domain_id, agent_id)

    def stats(self) -> dict:
        """S2 计数：故障恢复时间 recovery_ms、受影响任务数 affected_tasks。"""
        return {"recovery_ms": None, "affected_tasks": None, "members": len(self._members)}
