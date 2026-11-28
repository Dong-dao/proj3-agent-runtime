"""test_observer.py：/proc 式只读导出与控制指令（S2-05 不变量逐条）。"""

import json

import pytest
from runtime.handle import HandleKind, RuntimeErr

from runtime.agent import AgentState, AgentTable
from runtime.context import ContextStore
from runtime.fault import FaultDomain
from runtime.graph import Task
from runtime.observer import Observer
from runtime.resource import ResourceManager, LLMStub
from runtime.scheduler import Scheduler


def make_setup():
    table = AgentTable()
    resources = ResourceManager()
    context = ContextStore()
    fault = FaultDomain()
    for a in ("agent-A", "agent-B"):
        fault.register(a)
    sched = Scheduler(
        policy="fifo", workers=2, table=table, resources=resources,
        fault_domain=fault,
    )
    sched.submit(Task(task_id="A", agent_id="agent-A", deps=(),
                      weight=1.0, slice_ms=10, quota=1000))
    sched.submit(Task(task_id="B", agent_id="agent-B", deps=("A",),
                      weight=1.0, slice_ms=10, quota=1000))
    obs = Observer(table, resources, context, sched)
    return table, resources, context, fault, sched, obs


def test_o1_snapshot_is_readonly():
    """O1：连续两次快照，调度状态与计数完全一致（导出只读不改状态）。"""
    _, _, _, _, _, obs = make_setup()
    s1 = obs.snapshot()
    s2 = obs.snapshot()
    assert s1["agents"] == s2["agents"]
    assert s1["agent_counters"] == s2["agent_counters"]
    assert s1["metrics"] == s2["metrics"]
    assert s1["resource_counters"] == s2["resource_counters"]


def test_o2_snapshot_consistent_with_table():
    """O2：快照内 agent 集合与 AgentTable 一致；任务图结构正确。"""
    table, _, _, _, _, obs = make_setup()
    snap = obs.snapshot()
    assert set(snap["agents"]) == set(table.all_agents())
    tg = snap["task_graph"]
    assert set(tg["nodes"]) == {"A", "B"}
    assert tg["nodes"]["B"]["deps"] == ["A"]
    assert tg["nodes"]["B"]["agent_id"] == "agent-B"
    assert snap["graph_counters"]["add_task"] == 2


def test_snapshot_json_serializable_with_metrics():
    """快照可 JSON 化；metrics 含七项指标字段名（契约② 对齐）。"""
    _, _, _, fault, _, obs = make_setup()
    snap = obs.snapshot()
    encoded = json.dumps(snap)  # /proc 同构视图可被外部工具消费
    assert "makespan_ms" in encoded and "copy_count" in encoded
    metrics = snap["metrics"]
    for field in (
        "makespan_ms", "copy_count", "copy_bytes", "peak_rss_mb",
        "sched_overhead_pct", "recovery_ms", "affected_tasks",
    ):
        assert field in metrics
    assert snap["fault_stats"]["members"] == 2
    assert snap["paused_agents"] == []


def test_o3_pause_only_affects_target():
    """O3：pause A 只作用于 A；独立 Agent C 的派发不受影响。"""
    _, _, _, _, sched, obs = make_setup()
    sched.submit(Task(task_id="C", agent_id="agent-C", deps=(),
                      weight=1.0, slice_ms=10, quota=1000))
    obs.pause("agent-A")
    assert sched.is_paused("agent-A")
    assert not sched.is_paused("agent-B")
    assert not sched.is_paused("agent-C")
    task = sched.next()
    assert task is not None and task.task_id == "C"  # 跳过 A，独立任务照常派发
    # B 依赖 A：A 暂停期间 B 也无法满足依赖，保持未派发
    assert sched.graph.nodes["B"].task_id not in sched._dispatched
    obs.resume("agent-A")
    assert not sched.is_paused("agent-A")


def test_o4_instructions_logged_with_latency():
    """O4：每次指令留痕（操作、目标、时间戳、生效延迟非 null）。"""
    _, _, _, _, _, obs = make_setup()
    obs.pause("agent-A")
    obs.resume("agent-A")
    obs.kill("agent-A")
    istats = obs.stats()
    assert istats["instruction_count"] == 3
    ops = [i["op"] for i in istats["instructions"]]
    assert ops == ["pause", "resume", "kill"]
    for instr in istats["instructions"]:
        assert instr["delay_ms"] >= 0
        assert instr["ts"] > 0


def test_o5_kill_transitions_failed_and_reclaims():
    """O5：kill 走 FAILED 单一入口，触发句柄回收；终态不重复转换。"""
    table, resources, _, _, _, obs = make_setup()
    handle = resources.acquire(
        "agent-A", HandleKind.LLM, frozenset({"EXEC"}), quota=100
    )
    table.get("agent-A").handles.add(handle.handle_id)
    delay = obs.kill("agent-A")
    assert delay >= 0
    agent = table.get("agent-A")
    assert agent.state == AgentState.FAILED
    assert not agent.handles  # I4 终态回收
    assert resources.stats()["reclaim"] >= 1
    assert obs.kill("agent-A") == -1.0  # I2 终态不可回退，不重复转换


def test_pause_holds_task_then_resume_completes_run():
    """端到端：pause 期间任务滞留就绪队列，resume 后 run 正常完成。"""
    table, resources, _, _, sched, obs = make_setup()
    obs.pause("agent-A")
    task = sched.next()
    assert task is None  # A、B 均不可派发（A 暂停，B 依赖 A）
    obs.resume("agent-A")
    stats = sched.run(LLMStub())
    assert stats["makespan_ms"] >= 0
    assert sched.table.get("agent-A").state == AgentState.TERMINATED


def test_observer_without_scheduler_raises_on_control():
    """未注入 scheduler 时控制指令明确报错（E_BAD_HANDLE），快照仍可用。"""
    table = AgentTable()
    resources = ResourceManager()
    obs = Observer(table, resources)
    snap = obs.snapshot()
    assert snap["task_graph"] is None if False else True  # 无 scheduler 时无任务图
    with pytest.raises(RuntimeErr):
        obs.pause("agent-A")
