"""test_fault.py：故障域、注入开关、超时重试熔断（S2-03 不变量逐条）。"""

from runtime.agent import AgentState
from runtime.fault import FaultDomain
from runtime.graph import Task
from runtime.resource import LLMStub
from runtime.scheduler import Scheduler


def make_task(tid, agent, deps=(), slice_ms=10.0, quota=1000):
    return Task(
        task_id=tid, agent_id=agent, deps=tuple(deps),
        weight=1.0, slice_ms=slice_ms, quota=quota,
    )


def test_f1_injection_only_affects_member():
    """F1：注入只作用于域内成员；未登记成员的 inject 被拒。"""
    fd = FaultDomain()
    fd.register("agent-A")
    fd.inject("agent-A", "timeout")
    fd.inject("agent-outside", "timeout")  # 域外成员：注入被拒
    stats = fd.stats()
    assert stats["injected"] == ["agent-A"]
    task_outside = make_task("T1", "agent-outside", slice_ms=500)
    assert fd.should_fail(task_outside) is False


def test_f2_fail_goes_through_single_entry():
    """F2：失败经 scheduler.on_fail -> transition（终态 FAILED），无直接赋值。"""
    fd = FaultDomain(timeout_ms=100, breaker_threshold=99)
    fd.register("agent-X")
    fd.inject("agent-X")
    sched = Scheduler(policy="fifo", workers=2, fault_domain=fd)
    sched.submit(make_task("A", "agent-X", slice_ms=500))
    sched.run(LLMStub())
    assert sched.table.get("agent-X").state == AgentState.FAILED
    # 状态机计数里应有一次到 FAILED 的合法转换
    assert sched.table.stats()["transitions"] >= 2  # CREATED->READY->RUNNING->FAILED
    assert fd.stats()["failure_count"] >= 1


def test_f3_scheduler_advances_after_single_failure():
    """F3 + G2 关键验收：单 Agent 失败后其余任务继续推进。"""
    fd = FaultDomain(timeout_ms=100, breaker_threshold=99)
    for a in ("agent-X", "agent-Y"):
        fd.register(a)
    fd.inject("agent-X")
    sched = Scheduler(policy="fifo", workers=2, fault_domain=fd)
    sched.submit(make_task("A", "agent-X", slice_ms=500))  # 失败
    sched.submit(make_task("C", "agent-Y", slice_ms=10))   # 独立：必须完成
    stats = sched.run(LLMStub())
    assert sched.table.get("agent-Y").state == AgentState.TERMINATED
    assert sched.table.get("agent-X").state == AgentState.FAILED
    assert stats["makespan_ms"] > 0  # 调度器持续推进到 C 完成


def test_f4_failed_agent_handles_reclaimed():
    """F4/I4：进入 FAILED 触发 release_all，句柄归零。"""
    fd = FaultDomain(timeout_ms=100, breaker_threshold=99)
    fd.register("agent-X")
    fd.inject("agent-X")
    sched = Scheduler(policy="fifo", workers=1, fault_domain=fd)
    sched.submit(make_task("A", "agent-X", slice_ms=500))
    stats = sched.run(LLMStub())
    agent = sched.table.get("agent-X")
    assert agent.state == AgentState.FAILED
    assert not agent.handles  # 句柄表归零
    assert stats["reclaimed_handles"] >= 1


def test_f5_recovery_time_and_affected_tasks_collected():
    """F5：recovery_ms 非 null 且 > 0；affected_tasks 与预期一致。"""
    fd = FaultDomain(timeout_ms=100, breaker_threshold=99)
    for a in ("agent-X", "agent-Y", "agent-Z", "agent-W"):
        fd.register(a)
    fd.inject("agent-X")
    sched = Scheduler(policy="fifo", workers=2, fault_domain=fd)
    sched.submit(make_task("A", "agent-X", slice_ms=500))          # 派发点失败（t=0）
    sched.submit(make_task("D", "agent-Z", slice_ms=600))          # 独立长任务
    sched.submit(make_task("B", "agent-W", ("A", "D"), slice_ms=10))  # 下游：受影响
    stats = sched.run(LLMStub())
    fstats = fd.stats()
    assert fstats["recovery_ms"] is not None
    assert fstats["recovery_ms"] == 600.0  # B 等到 D 完成（t=600）才重新就绪
    assert fstats["affected_tasks"] >= 1   # B 因故障未能按时完成
    assert "B" in {  # 受影响登记可查
        t for t in ("A", "B", "D") if fd.is_affected(t)
    }
    assert stats["makespan_ms"] > 0


def test_retry_quota_allows_limited_attempts():
    """重试：同一任务最多判定 1 + max_retries 次，超过后放行。"""
    fd = FaultDomain(timeout_ms=100, breaker_threshold=99, max_retries=1)
    fd.register("agent-X")
    fd.inject("agent-X")
    task = make_task("A", "agent-X", slice_ms=500)
    assert fd.should_fail(task) is True   # 首次
    assert fd.should_fail(task) is True   # 重试 1 次
    assert fd.should_fail(task) is False  # 超过配额：放行（降级接受）
    assert fd.should_fail(task) is False


def test_breaker_opens_after_consecutive_failures():
    """熔断：连续失败达到阈值打开熔断器，is_broken 生效。"""
    fd = FaultDomain(breaker_threshold=2)
    fd.register("agent-X")
    fd.record_failure("agent-X", "timeout", now_ms=0.0)
    assert fd.is_broken("agent-X") is False
    fd.record_failure("agent-X", "timeout", now_ms=1.0)
    assert fd.is_broken("agent-X") is True
    assert "agent-X" in fd.stats()["breaker_open"]


def test_broken_agent_fails_fast_in_run():
    """熔断打开后该 Agent 后续任务不再执行 stub，直接走 on_fail。"""
    fd = FaultDomain(breaker_threshold=1)
    fd.register("agent-X")
    fd.record_failure("agent-X", "manual", now_ms=0.0)  # 直接打开熔断
    assert fd.is_broken("agent-X")
    sched = Scheduler(policy="fifo", workers=2, fault_domain=fd)
    sched.submit(make_task("A", "agent-X", slice_ms=10))
    sched.submit(make_task("C", "agent-Y", slice_ms=10))
    stats = sched.run(LLMStub())
    assert sched.table.get("agent-X").state == AgentState.FAILED
    assert sched.table.get("agent-Y").state == AgentState.TERMINATED  # 其余继续
    assert fd.stats()["failure_count"] >= 2


def test_stub_exception_isolated_to_domain():
    """stub 抛异常：该任务走 on_fail，其余任务继续（故障不跨执行域）。"""
    fd = FaultDomain()
    for a in ("agent-X", "agent-Y"):
        fd.register(a)
    sched = Scheduler(policy="fifo", workers=2, fault_domain=fd)

    class ExplodingStub(LLMStub):
        def run(self, key: str) -> str:
            if key.startswith("A:"):
                raise RuntimeError("boom")
            return super().run(key)

    sched.submit(make_task("A", "agent-X", slice_ms=10))
    sched.submit(make_task("C", "agent-Y", slice_ms=10))
    stats = sched.run(ExplodingStub())
    assert sched.table.get("agent-X").state == AgentState.FAILED
    assert sched.table.get("agent-Y").state == AgentState.TERMINATED
    assert fd.stats()["failure_count"] == 1


def test_pause_agent_keeps_scheduler_running():
    """pause 指令（调度器侧）：被暂停 Agent 任务不派发，其余继续；resume 恢复。"""
    sched = Scheduler(policy="fifo", workers=2)
    sched.submit(make_task("A", "agent-A", slice_ms=10))
    sched.submit(make_task("B", "agent-B", slice_ms=10))
    sched.pause_agent("agent-A")
    assert sched.is_paused("agent-A")
    task = sched.next()
    assert task is not None and task.agent_id == "agent-B"  # 跳过 A，派发 B
    sched.resume_agent("agent-A")
    assert not sched.is_paused("agent-A")
    task = sched.next()
    assert task is not None and task.agent_id == "agent-A"
    assert sched.next() is None
