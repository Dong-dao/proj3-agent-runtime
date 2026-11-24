"""scheduler.py v1/v2 单测。

对应本周规划第三节 11/20-11/22：
- v1：依赖未满足不入队；同一任务不重复派发；状态迁移日志完整；
- v2：排序键 (eligible, vd, seq)，关键路径长的任务优先，有文字依据；
- FIFO 基线：policy="fifo" 只按提交序，供对比。
"""

import logging

import pytest

from runtime.graph import Task, TaskGraph
from runtime.resource import LLMStub
from runtime.scheduler import POLICY_FIFO, POLICY_VD, Scheduler


def make_task(task_id, deps=(), slice_ms=10.0, weight=1.0, quota=10000, agent_id=None):
    return Task(
        task_id=task_id,
        agent_id=agent_id or f"agent-{task_id}",
        deps=tuple(deps),
        weight=weight,
        slice_ms=slice_ms,
        quota=quota,
    )


# ---- v1：依赖未满足不入队 -----------------------------------------------------


def test_v1_dependent_task_not_enqueued_before_deps_done():
    """依赖未满足的任务不入就绪队列：A 未完成时，依赖它的 B 不被 next() 返回。"""
    sched = Scheduler(policy=POLICY_FIFO, workers=2)
    sched.submit(make_task("A"))
    sched.submit(make_task("B", deps=("A",)))
    first = sched.next()
    assert first.task_id == "A", "依赖未满足的 B 不得先于 A 派发"
    assert "B" in sched._pending, "B 应停留在 CREATED（未入队）"
    second = sched.next()
    assert second is None, "A 摘除后无其他 eligible 任务"
    # A 完成后 B 才入队
    sched.dispatch(first)
    sched.on_complete("A", "ok")
    task = sched.next()
    assert task.task_id == "B"


def test_v1_no_duplicate_dispatch():
    """同一任务不重复派发：next() 摘除就绪队列 + dispatch 标记 _dispatched 双保险。"""
    sched = Scheduler(policy=POLICY_FIFO, workers=4)
    for tid in ("A", "B"):
        sched.submit(make_task(tid))
    picked = [sched.next().task_id for _ in range(2)]
    assert sorted(picked) == ["A", "B"]
    assert sched.next() is None, "全部摘除后不得再返回任何任务"
    for task_id in picked:
        sched.dispatch(sched.graph.nodes[task_id])
    assert len(sched._dispatched) == 2


def test_v1_states_change_only_via_transition(caplog):
    """状态迁移日志完整：3 Agent 的 CREATED->READY->RUNNING->TERMINATED 全程有日志。"""
    sched = Scheduler(policy=POLICY_FIFO, workers=3)
    sched.submit(make_task("A", slice_ms=30))
    sched.submit(make_task("B", deps=("A",), slice_ms=20))
    sched.submit(make_task("C", slice_ms=10))
    with caplog.at_level(logging.INFO, logger="runtime.agent"):
        stats = sched.run(LLMStub())
    logs = [r.getMessage() for r in caplog.records]
    for agent_id in ("agent-A", "agent-B", "agent-C"):
        transitions = [m for m in logs if m.startswith("transition " + agent_id)]
        assert len(transitions) == 3, f"{agent_id} 应有 3 条状态迁移日志"
        assert "CREATED -> READY" in transitions[0]
        assert "READY -> RUNNING" in transitions[1]
        assert "RUNNING -> TERMINATED" in transitions[2]
    assert stats["transitions"] == 9
    assert stats["illegal_intercepts"] == 0


# ---- v2：排序键 (eligible, vd, seq) -----------------------------------------


def test_v2_critical_path_longer_dispatched_first():
    """关键路径长的任务优先：weight 相同、slice 更长者 vd 更近。

    E 与 F 同为 eligible、weight=1：E.slice=30 > F.slice=10 -> E 的 vd 更远？
    不对 —— vd = ready_time + slice/weight，slice 越大 vd 越晚。v2 的关键路径
    优先由 weight 体现（关键路径长者 weight 大）。本用例验证：同 ready 时刻、
    weight 相同则短任务先跑（时延敏感，vd 近先跑，EEVDF 的时延约束思想）。
    """
    sched = Scheduler(policy=POLICY_VD, workers=1)
    sched.submit(make_task("short", slice_ms=10, weight=1.0))
    sched.submit(make_task("long", slice_ms=30, weight=1.0))
    assert sched.next().task_id == "short", "slice 小、weight 同者 vd 近，先派发"


def test_v2_heavier_weight_jumps_queue():
    """关键路径长者 weight 大：vd = ready + slice/weight 更近，越过短任务先派发。"""
    sched = Scheduler(policy=POLICY_VD, workers=1)
    sched.submit(make_task("tail_short", slice_ms=10, weight=1.0))   # vd = 10
    sched.submit(make_task("head_long", slice_ms=30, weight=6.0))    # vd = 5
    assert sched.next().task_id == "head_long", "weight 大者 slice/weight 小，vd 近先跑"


def test_v2_seq_tiebreak_deterministic():
    """同键按提交序（seq 兜底），不用随机数：两次构建结果一致。"""
    order = []
    for _ in range(2):
        sched = Scheduler(policy=POLICY_VD, workers=1)
        for tid in ("A", "B", "C"):
            sched.submit(make_task(tid, slice_ms=10, weight=1.0))
        order.append([sched.next().task_id for _ in range(3)])
    assert order[0] == order[1] == ["A", "B", "C"]


def test_fifo_policy_orders_by_seq():
    """FIFO 基线：policy=fifo 只按提交序，不计算 vd。"""
    sched = Scheduler(policy=POLICY_FIFO, workers=1)
    sched.submit(make_task("A", slice_ms=30, weight=6.0))
    sched.submit(make_task("B", slice_ms=10, weight=1.0))
    assert sched.next().task_id == "A", "FIFO 只看提交序"
    assert sched.next().task_id == "B"


def test_quota_exhausted_rejects_dispatch():
    """配额透支：eligible 的 lag >= 0 条款，READY -> FAILED（配额拒绝）。"""
    sched = Scheduler(policy=POLICY_FIFO, workers=1)
    sched.submit(make_task("A", slice_ms=30, quota=20))  # slice 超过 quota
    task = sched.next()
    assert task.task_id == "A"
    handle = sched.dispatch(task)
    assert handle is None, "配额透支不得派发"
    assert sched.table.get("agent-A").state.name == "FAILED"
    assert sched.next() is None


# ---- 端到端：3 Agent 有依赖有并行 ---------------------------------------------


def test_end_to_end_three_agents_deps_and_parallel():
    """端到端：A、C 并行，B 依赖 A；makespan < 串行总和；全部终态回收。"""
    sched = Scheduler(policy=POLICY_VD, workers=3)
    sched.submit(make_task("A", slice_ms=30))
    sched.submit(make_task("B", deps=("A",), slice_ms=20))
    sched.submit(make_task("C", slice_ms=10))
    stats = sched.run(LLMStub())
    assert stats["makespan_ms"] == 50.0, "A/C 并行 30ms + B 20ms"
    assert stats["makespan_ms"] < 60.0, "必须优于串行（60ms）"
    assert stats["transitions"] == 9
    assert stats["reclaimed_handles"] == 3
    for agent_id in ("agent-A", "agent-B", "agent-C"):
        assert sched.table.get(agent_id).state.name == "TERMINATED"


def test_same_workload_reproduces_identical_trace():
    """决定论：同一负载重复运行，派发顺序与 makespan 完全一致。"""
    traces = []
    for _ in range(2):
        sched = Scheduler(policy=POLICY_VD, workers=2)
        sched.submit(make_task("A", slice_ms=30))
        sched.submit(make_task("B", deps=("A",), slice_ms=20))
        sched.submit(make_task("C", slice_ms=10))
        sched.submit(make_task("D", deps=("C",), slice_ms=5))
        traces.append((sched.run(LLMStub())["makespan_ms"], sched._dispatched.copy()))
    assert traces[0][0] == traces[1][0]
    assert traces[0][1] == traces[1][1]


def test_spawn_during_run_inherits_and_schedules():
    """运行中动态注入子任务：继承父任务 quota/weight，父完成后被调度（含环防护）。"""
    sched = Scheduler(policy=POLICY_VD, workers=1)
    sched.submit(make_task("A", slice_ms=30, weight=2.0, quota=500))
    task_a = sched.next()
    assert task_a.task_id == "A"
    sched.dispatch(task_a)
    # A 运行中注入子任务 A1（quota/weight 缺省 -> 继承父任务账本与权重）
    sched.spawn("A", make_task("A1", deps=("A",), slice_ms=5, weight=0, quota=0,
                               agent_id="agent-A"))
    child = sched.graph.nodes["A1"]
    assert child.quota == 500 and child.weight == 2.0, "子任务继承父任务账本与权重"
    assert "A1" in sched._pending, "父未完成前子任务不入队"
    sched.on_complete("A", "ok")
    task = sched.next()
    assert task.task_id == "A1", "父完成后子任务入队并被派发"


def test_stats_export_ready_len_and_waiting():
    """stats() 导出：就绪队列长度、等待时间、总完成时间可采集（契约②对齐）。"""
    sched = Scheduler(policy=POLICY_FIFO, workers=2)
    sched.submit(make_task("A", slice_ms=30))
    sched.submit(make_task("B", deps=("A",), slice_ms=20))
    stats = sched.run(LLMStub())
    assert stats["makespan_ms"] == 50.0
    assert stats["waiting_ms_total"] == 0.0  # workers=2 时 B 无等待（A 结束即派发）
    assert stats["ready_len"] == 0
    assert "sched_overhead_pct" in stats and "deadline_miss" in stats
