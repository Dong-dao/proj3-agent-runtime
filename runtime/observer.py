"""observer.py：/proc 式只读导出与控制面（S2 机制⑤，S2-05 设计说明冻结口径）。

机制：
- 只读快照 snapshot()：导出任务图（DAG 结构）、各 Agent 状态与配额、句柄表、
  上下文引用、七项指标当前值、故障域状态——某一时刻的一致视图，
  导出动作本身不改变调度状态（O1 只读，O2 一致）；
- 控制指令 pause / resume / kill：统一入口 _control，只作用于目标 Agent
  （O3），每次指令记日志与时间戳并测生效延迟（O4）；kill 走
  transition -> FAILED 单一入口，触发句柄回收，与故障域熔断同一路径（O5）。

OS 出处：/proc 与 cgroup 统计接口——只读文件视图 + kill/stop/cont 控制信号。
是同构只读 JSON 视图，不挂真实 procfs；控制是用户态逻辑，不产生真实信号
（边界声明见 S2-05 第 7 节）。
"""

from __future__ import annotations

import logging
import time

from .handle import RuntimeErr

log = logging.getLogger("runtime.observer")


class Observer:
    """只读状态导出（/proc 同构视图）+ 受控指令入口。"""

    def __init__(self, table, resources, context_store=None, scheduler=None) -> None:
        self._table = table
        self._resources = resources
        self._context = context_store
        self._scheduler = scheduler
        self._instructions: list[dict] = []  # 控制指令留痕（O4）

    # ---- 只读快照 --------------------------------------------------------------
    def snapshot(self) -> dict:
        """只读快照：导出不改状态（P5 可观测即接口）。

        S2 补充：任务图 DAG 快照、图计数、故障域状态、暂停集合，
        以及七项指标当前值 metrics（字段名与契约②一致）。
        """
        agents = {
            agent_id: {
                "state": agent.state.name,
                "quota": agent.quota,
                "handles": sorted(agent.handles),
                "ctx_refs": sorted(agent.ctx_refs),
            }
            for agent_id, agent in self._table.all_agents().items()
        }
        snap: dict = {
            "agents": agents,
            "agent_counters": self._table.stats(),
            "resource_counters": self._resources.stats(),
            "context_stats": self._context.stats() if self._context else None,
            "scheduler_stats": self._scheduler.stats() if self._scheduler else None,
        }
        if self._scheduler is not None:
            graph = self._scheduler.graph
            snap["task_graph"] = {
                "nodes": {
                    tid: {
                        "agent_id": t.agent_id,
                        "deps": sorted(t.deps),
                        "slice_ms": t.slice_ms,
                        "weight": t.weight,
                    }
                    for tid, t in graph.nodes.items()
                },
                "edges": {tid: sorted(deps) for tid, deps in graph.edges.items()},
            }
            snap["graph_counters"] = graph.stats()
            snap["paused_agents"] = sorted(getattr(self._scheduler, "_paused", set()))
            fd = getattr(self._scheduler, "fault_domain", None)
            snap["fault_stats"] = fd.stats() if fd is not None else None
        snap["metrics"] = self._metrics_snapshot(snap)
        return snap

    def _metrics_snapshot(self, snap: dict) -> dict:
        """七项指标当前值（契约② 字段名），从各模块 stats 汇集，只读。"""
        sched = snap.get("scheduler_stats") or {}
        ctx = snap.get("context_stats") or {}
        fault = snap.get("fault_stats") or {}
        return {
            "makespan_ms": sched.get("makespan_ms"),
            "copy_count": ctx.get("copy_count"),
            "copy_bytes": ctx.get("copy_bytes"),
            "peak_rss_mb": None,  # 采样线程采集（S3 全量对比用，S2 只留字段位）
            "sched_overhead_pct": sched.get("sched_overhead_pct"),
            "recovery_ms": fault.get("recovery_ms"),
            "affected_tasks": fault.get("affected_tasks"),
        }

    # ---- 控制指令（统一入口） -----------------------------------------------------
    def _control(self, op: str, agent_id: str, apply) -> float:
        """控制指令统一入口：校验目标、留痕日志、测生效延迟（O3/O4）。

        返回生效延迟（毫秒）：指令时刻到状态变更完成的耗时。
        """
        t0 = time.perf_counter()
        log.info("control %s %s: 指令下发 (ts=%.6f)", op, agent_id, time.time())
        apply()  # 实际生效动作（O3：只作用于目标 Agent）
        delay_ms = (time.perf_counter() - t0) * 1000.0
        self._instructions.append(
            {"op": op, "agent_id": agent_id, "delay_ms": delay_ms, "ts": time.time()}
        )
        log.info("control %s %s: 生效，延迟 %.3fms", op, agent_id, delay_ms)
        return delay_ms

    def pause(self, agent_id: str) -> float:
        """暂停目标 Agent 的任务派发（任务留在就绪队列，其他 Agent 不受影响）。"""
        if self._scheduler is None:
            raise RuntimeErr("E_BAD_HANDLE", "Observer 未注入 scheduler，无法 pause")
        return self._control(
            "pause", agent_id, lambda: self._scheduler.pause_agent(agent_id)
        )

    def resume(self, agent_id: str) -> float:
        """恢复目标 Agent 的任务派发。"""
        if self._scheduler is None:
            raise RuntimeErr("E_BAD_HANDLE", "Observer 未注入 scheduler，无法 resume")
        return self._control(
            "resume", agent_id, lambda: self._scheduler.resume_agent(agent_id)
        )

    def kill(self, agent_id: str) -> float:
        """终止目标 Agent：走 transition -> FAILED 单一入口（O5），触发句柄回收。

        已处终态的 Agent 不再重复转换（I2 终态不可回退），记 WARNING 返回 -1。
        """
        from .agent import AgentState
        from .handle import RuntimeErr, E_ILLEGAL_TRANSITION

        state = self._table.get(agent_id).state
        if state in (AgentState.TERMINATED, AgentState.FAILED):
            log.warning(
                "control kill %s: 已处终态 %s，不重复转换（I2）", agent_id, state.name
            )
            return -1.0

        def _apply() -> None:
            try:
                self._table.transition(agent_id, AgentState.FAILED)
            except RuntimeErr as exc:
                if exc.code != E_ILLEGAL_TRANSITION:
                    raise
                log.warning("control kill %s: 转换被拒（%s）", agent_id, exc)

        return self._control("kill", agent_id, _apply)

    # ---- 指令留痕导出 --------------------------------------------------------------
    def stats(self) -> dict:
        """控制指令留痕：每次指令的操作、目标、生效延迟与时间戳（O4）。"""
        return {
            "instructions": list(self._instructions),
            "instruction_count": len(self._instructions),
        }
