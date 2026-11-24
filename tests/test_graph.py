"""graph.py 依赖 DAG 单测。

对应本周规划第三节 11/19 与第四节步骤 4：
python -m pytest -q tests/test_graph.py -k cycle
验收：含环用例报错（E_DAG_CYCLE 日志可见）；动态子任务可注入；成环的
动态子任务被拒绝（死锁防护）。

环的来源说明：deps 必须指向已存在任务时，add_task / spawn 新节点自身是汇点，
理论上不会成环；真正的环入口是 add_edge 回边，以及「子任务自依赖」这类
非法输入。add_task / spawn 里的环检测是契约要求的防御网（E_DAG_CYCLE），
本测试同时覆盖真实入口与防御网两条路径。
"""

import logging

import pytest

from runtime.graph import Task, TaskGraph
from runtime.handle import E_DAG_CYCLE, RuntimeErr


def make_task(task_id, deps=(), slice_ms=10.0, weight=1.0, quota=100, agent_id=None):
    return Task(
        task_id=task_id,
        agent_id=agent_id or f"agent-{task_id}",
        deps=tuple(deps),
        weight=weight,
        slice_ms=slice_ms,
        quota=quota,
    )


def force_edge(graph: TaskGraph, predecessor: str, successor: str) -> None:
    """绕过 add_edge 保护直接注入一条边（仅测试用，用于构造环验证检测能力）。"""
    graph.edges[successor] = graph.edges[successor] | {predecessor}
    graph._dependents.setdefault(predecessor, set()).add(successor)


# ---- 步骤 4 验收：含环用例报错 -----------------------------------------------


def test_cycle_detected_returns_path():
    """detect_cycle() 对 A->B->C->A 返回首尾相同的环路径；无环返回 None。"""
    graph = TaskGraph()
    graph.add_task(make_task("A"))
    graph.add_task(make_task("B", deps=("A",)))
    graph.add_task(make_task("C", deps=("B",)))
    assert graph.detect_cycle() is None
    force_edge(graph, "C", "A")  # 制造 A->B->C->A 环
    cycle = graph.detect_cycle()
    assert cycle is not None
    assert cycle[0] == cycle[-1], "环路径首尾相同"


def test_cycle_add_edge_rejected_with_log(caplog):
    """add_edge 回边成环：抛 E_DAG_CYCLE、留拒绝日志、边不入图。"""
    graph = TaskGraph()
    graph.add_task(make_task("A"))
    graph.add_task(make_task("B", deps=("A",)))
    graph.add_task(make_task("C", deps=("B",)))
    with caplog.at_level(logging.WARNING, logger="runtime.graph"):
        with pytest.raises(RuntimeErr) as exc_info:
            graph.add_edge("C", "A")  # A 依赖 C，而 C 经 B 依赖 A -> 环
    assert exc_info.value.code == E_DAG_CYCLE
    cycle_logs = [r for r in caplog.records if "E_DAG_CYCLE" in r.getMessage()]
    assert cycle_logs, "成环拒绝必须留 E_DAG_CYCLE 日志"
    assert "C" not in graph.edges["A"], "被拒绝的边不得留在图里"
    assert graph.detect_cycle() is None, "拒绝后图必须保持无环"


def test_cycle_add_task_self_dependency_rejected():
    """子任务自依赖（deps 指向自己）：不入图，报错。"""
    graph = TaskGraph()
    graph.add_task(make_task("A"))
    with pytest.raises(ValueError):
        graph.add_task(make_task("A", deps=("A",)))


def test_cycle_defensive_net_on_spawn(caplog):
    """防御网：图中已存在环时 spawn 拒绝注入并回滚（契约：spawn 后重测环）。"""
    graph = TaskGraph()
    graph.add_task(make_task("A"))
    graph.add_task(make_task("B", deps=("A",)))
    force_edge(graph, "B", "A")  # 注入环（模拟上游契约破坏）
    with caplog.at_level(logging.WARNING, logger="runtime.graph"):
        with pytest.raises(RuntimeErr) as exc_info:
            graph.spawn("A", make_task("A1", deps=("A",), agent_id="agent-A"))
    assert exc_info.value.code == E_DAG_CYCLE
    cycle_logs = [r for r in caplog.records if "E_DAG_CYCLE" in r.getMessage()]
    assert cycle_logs
    assert "A1" not in graph.nodes, "被拒绝的子任务必须回滚出图"


def test_cycle_spawn_rejects_illegal_chain():
    """spawn 的子任务 deps 指向不在图中的任务：拒绝注入。"""
    graph = TaskGraph()
    graph.add_task(make_task("A"))
    with pytest.raises(ValueError):
        graph.spawn("A", make_task("A1", deps=("ghost",)))


# ---- 11/19 验收：动态子任务可注入 --------------------------------------------


def test_spawn_dynamic_subtask_injected():
    """动态子任务可注入：spawn 后入图，依赖父任务完成后进入 ready_set。"""
    graph = TaskGraph()
    graph.add_task(make_task("A", slice_ms=30))
    graph.spawn("A", make_task("A1", deps=("A",), slice_ms=5, agent_id="agent-A"))
    assert "A1" in graph.nodes
    assert graph.detect_cycle() is None
    # A 未完成时 A1 不就绪
    assert graph.ready_set(frozenset()) == ("A",)
    # A 完成后 A1 就绪
    assert graph.ready_set(frozenset({"A"})) == ("A", "A1")


def test_spawn_inherits_quota_and_weight():
    """spawn 子任务继承父任务的 quota 账本与 weight 基线（quota/weight 缺省时）。"""
    graph = TaskGraph()
    graph.add_task(make_task("A", weight=3.0, quota=250))
    graph.spawn("A", make_task("A1", deps=("A",), weight=0, quota=0))
    child = graph.nodes["A1"]
    assert child.quota == 250, "quota 缺省（<=0）时继承父任务"
    assert child.weight == 3.0, "weight 缺省（<=0）时继承父任务基线"
    # 显式声明的子任务不覆盖继承
    graph.spawn("A", make_task("A2", deps=("A",), weight=9.0, quota=99))
    assert graph.nodes["A2"].quota == 99
    assert graph.nodes["A2"].weight == 9.0


def test_ready_set_filters_by_done():
    """ready_set：deps 全部在 done 中才返回（依赖未满足不入队的图侧依据）。"""
    graph = TaskGraph()
    graph.add_task(make_task("A"))
    graph.add_task(make_task("B"))
    graph.add_task(make_task("C", deps=("A", "B")))
    graph.add_task(make_task("D", deps=("C",)))
    assert graph.ready_set(frozenset()) == ("A", "B")
    assert graph.ready_set(frozenset({"A", "B"})) == ("A", "B", "C")
    assert graph.ready_set(frozenset({"A", "B", "C", "D"})) == ("A", "B", "C", "D")


def test_critical_path_remaining():
    """critical_path_remaining：到汇点的最长 slice 链长（含自身）。"""
    graph = TaskGraph()
    graph.add_task(make_task("A", slice_ms=30))
    graph.add_task(make_task("B", deps=("A",), slice_ms=20))
    graph.add_task(make_task("C", slice_ms=10))
    assert graph.critical_path_remaining("A") == 50.0  # A + B
    assert graph.critical_path_remaining("B") == 20.0
    assert graph.critical_path_remaining("C") == 10.0


def test_duplicate_task_rejected():
    """task_id 全局唯一：重复登记被拒绝。"""
    graph = TaskGraph()
    graph.add_task(make_task("A"))
    with pytest.raises(ValueError):
        graph.add_task(make_task("A"))


def test_missing_dep_rejected():
    """deps 引用的前置任务必须已在图中。"""
    graph = TaskGraph()
    with pytest.raises(ValueError):
        graph.add_task(make_task("B", deps=("ghost",)))
