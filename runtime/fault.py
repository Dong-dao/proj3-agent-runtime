"""fault.py：故障域、注入开关、超时重试熔断（S2 机制③，S2-03 设计说明冻结口径）。

故障域边界（关键设计决策，S2-03 第 2 节，答辩必答项）：
- 域内：各 Agent 执行体（可能超时、抛异常，是故障源）；
- 域外：scheduler.py（控制面，必须始终可推进）、bus.py（故障后仍需通信）、
  observer.py（故障时要能导出现场）、resource.py（故障后要能回收句柄）。
一句话：以 Agent 执行域为界，调度器与总线在域外。

超时熔断流程（S2-03 第 3 节）：
    注入开关命中 / 超时
      -> FaultDomain.record_failure 记故障时刻与连续失败数
      -> Scheduler.on_fail 走 RUNNING -> FAILED 单一入口（终态回收句柄）
      -> 依赖它的下游任务登记为受影响，故障后重新就绪（降级重试，不直接置 FAILED）
      -> 其余就绪任务继续派发
      -> 受影响任务重新就绪时刻 - 故障时刻 = recovery_ms
      -> 未能按时完成的任务数 = affected_tasks

重试与熔断（用户态逻辑判定，不是内核机制）：
- 重试：should_fail 对同一任务最多判定 1 + max_retries 次，之后放行
  （下游重试语义：故障后重新就绪即重试）；
- 熔断：同一 Agent 连续失败 >= breaker_threshold 时打开熔断器，
  该 Agent 后续任务不再执行 stub，直接走 on_fail 失败路径。

不变量（S2-03 第 4 节）：
- F1 故障只影响域内 Agent；
- F2 失败走单一入口（scheduler.on_fail -> transition）；
- F3 调度器故障后仍推进；
- F4 进入 FAILED 触发 release_all 回收；
- F5 recovery_ms 可采（故障与重新就绪各打时间戳）。

OS 出处：namespace / cgroup 的故障隔离边界——迁移「故障域」概念：
把可能失败的执行体圈在域内，域外控制面保持存活。是进程内逻辑隔离，
不是内核级资源隔离（边界声明见 S2-03 第 7 节）。
"""

from __future__ import annotations

import logging

log = logging.getLogger("runtime.fault")


class FaultDomain:
    """故障域：登记域内成员、注入开关、超时判定、重试配额与熔断器。"""

    def __init__(
        self,
        domain_id: str = "default",
        timeout_ms: float | None = None,
        breaker_threshold: int = 2,
        max_retries: int = 1,
    ) -> None:
        self.domain_id = domain_id
        self._timeout_ms = timeout_ms
        self._breaker_threshold = max(1, int(breaker_threshold))
        self._max_retries = max(0, int(max_retries))
        self._members: set[str] = set()
        self._injected: dict[str, str] = {}      # agent_id -> 注入原因（注入开关）
        self._fail_counts: dict[str, int] = {}   # agent_id -> 连续失败次数（熔断判定）
        self._breaker_open: set[str] = set()     # 熔断器打开的 agent 集合
        self._attempts: dict[str, int] = {}      # task_id -> 失败判定次数（重试配额）
        self._affected: set[str] = set()         # 受影响任务（未能按时完成）
        self._fault_time: float | None = None    # 首次故障时刻（调度模拟时钟，ms）
        self._recovered_time: float | None = None  # 受影响任务重新就绪时刻
        self._failure_count = 0

    # ---- 成员与注入开关 -------------------------------------------------------
    def register(self, agent_id: str) -> None:
        """登记域内成员（F1：熔断与注入只作用于成员）。"""
        self._members.add(agent_id)
        log.debug("fault domain %s: registered %s", self.domain_id, agent_id)

    def inject(self, agent_id: str, reason: str = "timeout") -> None:
        """打开注入开关：该 Agent 的任务按 should_fail 判定失败（可测、决定论）。"""
        if agent_id not in self._members:
            log.warning(
                "inject %s 被拒：不在故障域 %s 内（F1 故障不跨执行域）",
                agent_id, self.domain_id,
            )
            return
        self._injected[agent_id] = reason
        log.info("fault domain %s: 注入开关打开 %s (%s)", self.domain_id, agent_id, reason)

    def clear_injection(self, agent_id: str) -> None:
        """关闭注入开关（不影响已打开的熔断器）。"""
        self._injected.pop(agent_id, None)
        log.info("fault domain %s: 注入开关关闭 %s", self.domain_id, agent_id)

    # ---- 失败判定 --------------------------------------------------------------
    def should_fail(self, task) -> bool:
        """对任务做失败判定：注入开关 + 超时判定 + 重试配额（决定论，不掷骰子）。

        - 任务所属 Agent 未被注入 → False；
        - 超时口径：task.slice_ms > timeout_ms 视为超时（未超时放行）；
        - 同一任务最多判定 1 + max_retries 次，超过后放行（避免死循环，
          放行语义对应「重试后降级接受」）。
        """
        reason = self._injected.get(task.agent_id)
        if reason is None:
            return False
        n = self._attempts.get(task.task_id, 0) + 1
        if n > 1 + self._max_retries:
            return False
        self._attempts[task.task_id] = n
        if self._timeout_ms is not None and task.slice_ms <= self._timeout_ms:
            return False
        log.info(
            "should_fail %s (agent=%s, 尝试 %d/%d): %s",
            task.task_id, task.agent_id, n, 1 + self._max_retries, reason,
        )
        return True

    def fail_reason(self, task) -> str:
        """返回该任务的注入原因（日志与指标归因用）。"""
        return self._injected.get(task.agent_id, "timeout")

    def is_broken(self, agent_id: str) -> bool:
        """熔断器是否打开：打开后该 Agent 任务不再执行 stub，直接失败。"""
        return agent_id in self._breaker_open

    # ---- 故障记账 --------------------------------------------------------------
    def record_failure(self, agent_id: str, reason: str, now_ms: float | None = None) -> None:
        """记一次故障：时间戳（F5）、连续失败计数、熔断器判定。

        时刻单位与调度器模拟时钟一致（毫秒）；now_ms 为 None 时用 wall clock 兜底。
        """
        import time as _time

        self._failure_count += 1
        self._fail_counts[agent_id] = self._fail_counts.get(agent_id, 0) + 1
        ts = now_ms if now_ms is not None else _time.time() * 1000.0
        if self._fault_time is None:
            self._fault_time = ts
        if (
            agent_id in self._members
            and self._fail_counts[agent_id] >= self._breaker_threshold
            and agent_id not in self._breaker_open
        ):
            self._breaker_open.add(agent_id)
            log.warning(
                "E_BREAKER 熔断器打开 %s: 连续失败 %d 次（阈值 %d），后续任务直接失败",
                agent_id, self._fail_counts[agent_id], self._breaker_threshold,
            )
        log.warning("fault %s: %s (第 %d 次)", agent_id, reason, self._failure_count)

    def mark_affected(self, task_id: str) -> None:
        """登记受影响任务（口径：未能按时完成的任务，不是被杀死的任务）。"""
        self._affected.add(task_id)

    def mark_downstream(self, task_ids) -> None:
        """批量登记受影响的下游任务。"""
        self._affected.update(task_ids)

    def mark_recovered(self, now_ms: float) -> None:
        """受影响任务重新就绪时刻（只记第一次）；recovery_ms = 该时刻 - 故障时刻。"""
        if self._recovered_time is None and self._fault_time is not None:
            self._recovered_time = now_ms
            log.info(
                "fault domain %s: 受影响任务重新就绪，recovery_ms=%.1f",
                self.domain_id, self._recovered_time - self._fault_time,
            )

    def is_affected(self, task_id: str) -> bool:
        return task_id in self._affected

    # ---- 计数导出 --------------------------------------------------------------
    def stats(self) -> dict:
        """契约② 字段：recovery_ms / affected_tasks（+ 诊断量）。

        recovery_ms = 受影响任务重新就绪时刻 - 故障时刻；未发生故障为 None。
        affected_tasks = 未能按时完成的任务数（S2-03 第 5 节定义提醒）。
        """
        recovery_ms = None
        if self._fault_time is not None and self._recovered_time is not None:
            recovery_ms = self._recovered_time - self._fault_time
        return {
            "recovery_ms": recovery_ms,
            "affected_tasks": len(self._affected),
            "members": len(self._members),
            "failure_count": self._failure_count,
            "breaker_open": sorted(self._breaker_open),
            "injected": sorted(self._injected),
        }
