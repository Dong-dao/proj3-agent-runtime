"""agent.py 六态状态机单测。

对应本周规划第三节 11/17 与第四节步骤 3：
python -m pytest -q tests/test_agent.py -k illegal_transition
验收：非法转换被拒绝并有日志；6 种状态齐全；终态不可回退；终态回收。
"""

import logging

import pytest

from runtime.agent import AgentState, AgentTable, transition
from runtime.handle import E_ILLEGAL_TRANSITION, HandleKind, RuntimeErr
from runtime.resource import ResourceManager


def make_table() -> AgentTable:
    return AgentTable()


# ---- 11/17 验收：6 种状态齐全 -----------------------------------------------


def test_six_states_defined():
    """六态齐全：CREATED / READY / RUNNING / BLOCKED / TERMINATED / FAILED。"""
    assert [s.name for s in AgentState] == [
        "CREATED", "READY", "RUNNING", "BLOCKED", "TERMINATED", "FAILED",
    ]


# ---- 步骤 3 验收：非法转换被拒绝并有日志 -------------------------------------


def test_illegal_transition_rejected_with_log(caplog):
    """CREATED -> RUNNING 非法：抛 E_ILLEGAL_TRANSITION 且拒绝日志可见。"""
    table = make_table()
    table.create("a1")
    with caplog.at_level(logging.WARNING, logger="runtime.agent"):
        with pytest.raises(RuntimeErr) as exc_info:
            table.transition("a1", AgentState.RUNNING)
    assert exc_info.value.code == E_ILLEGAL_TRANSITION
    illegal_logs = [r for r in caplog.records if "E_ILLEGAL_TRANSITION" in r.getMessage()]
    assert illegal_logs, "非法转换必须留下一条拒绝日志"
    assert table.stats()["illegal_intercepts"] == 1
    # 状态必须保持不变（I1 状态唯一）
    assert table.get("a1").state == AgentState.CREATED


def test_illegal_transition_blocked_to_running(caplog):
    """BLOCKED -> RUNNING 非法（I3 阻塞只回就绪）。"""
    table = make_table()
    table.create("a1")
    table.transition("a1", AgentState.READY)
    table.transition("a1", AgentState.RUNNING)
    table.transition("a1", AgentState.BLOCKED)
    with pytest.raises(RuntimeErr) as exc_info:
        table.transition("a1", AgentState.RUNNING)
    assert exc_info.value.code == E_ILLEGAL_TRANSITION
    assert table.get("a1").state == AgentState.BLOCKED


# ---- 6x6 矩阵逐格覆盖 -------------------------------------------------------


LEGAL = {
    AgentState.CREATED: {AgentState.READY, AgentState.FAILED},
    AgentState.READY: {AgentState.READY, AgentState.RUNNING, AgentState.FAILED},
    AgentState.RUNNING: {AgentState.BLOCKED, AgentState.TERMINATED, AgentState.FAILED},
    AgentState.BLOCKED: {AgentState.READY, AgentState.FAILED},
    AgentState.TERMINATED: set(),
    AgentState.FAILED: set(),
}


def _drive_to(table: AgentTable, agent_id: str, state: AgentState) -> None:
    """按合法路径把 Agent 驱动到指定状态。"""
    path = {
        AgentState.CREATED: [],
        AgentState.READY: [AgentState.READY],
        AgentState.RUNNING: [AgentState.READY, AgentState.RUNNING],
        AgentState.BLOCKED: [AgentState.READY, AgentState.RUNNING, AgentState.BLOCKED],
        AgentState.TERMINATED: [AgentState.READY, AgentState.RUNNING, AgentState.TERMINATED],
        AgentState.FAILED: [AgentState.FAILED],
    }[state]
    for step in path:
        table.transition(agent_id, step)


def test_full_transition_matrix_covered():
    """6x6 矩阵逐格覆盖：允许的转换成功，禁止的转换全部抛 E_ILLEGAL_TRANSITION。"""
    for from_state, allowed in LEGAL.items():
        for to_state in AgentState:
            table = make_table()
            agent_id = f"agent-{from_state.name.lower()}"
            table.create(agent_id)
            _drive_to(table, agent_id, from_state)
            if to_state in allowed:
                table.transition(agent_id, to_state)
                assert table.get(agent_id).state == to_state
            else:
                with pytest.raises(RuntimeErr) as exc_info:
                    table.transition(agent_id, to_state)
                assert exc_info.value.code == E_ILLEGAL_TRANSITION
                assert table.get(agent_id).state == from_state


def test_terminal_states_irreversible():
    """I2 终态不可回退：TERMINATED / FAILED 对任意目标（含自环）全部拒绝。"""
    for terminal in (AgentState.TERMINATED, AgentState.FAILED):
        table = make_table()
        table.create("a1")
        _drive_to(table, "a1", terminal)
        for to_state in AgentState:
            with pytest.raises(RuntimeErr):
                table.transition("a1", to_state)


def test_module_level_transition_entry():
    """全局 transition() 单一入口可用，非法转换同样拒绝。"""
    table = make_table()
    table.create("a1")
    transition("a1", AgentState.READY, table=table)
    assert table.get("a1").state == AgentState.READY
    with pytest.raises(RuntimeErr):
        transition("a1", AgentState.BLOCKED, table=table)


def test_concurrent_transition_state_unique():
    """I1 状态唯一：并发发起两次相同转换，只生效一次（第二次被矩阵拒绝）。"""
    import threading

    table = make_table()
    table.create("a1")
    table.transition("a1", AgentState.READY)
    results = []

    def worker():
        try:
            table.transition("a1", AgentState.RUNNING)
            results.append("ok")
        except RuntimeErr:
            results.append("rejected")

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 第一个 READY -> RUNNING 合法；第二个到达时已是 RUNNING，
    # 而 RUNNING -> RUNNING 不在矩阵内，必须被拒绝
    assert table.get("a1").state == AgentState.RUNNING
    assert results.count("ok") == 1
    assert results.count("rejected") == 1


# ---- I4 终态回收 -------------------------------------------------------------


def test_terminal_state_reclaims_all_handles():
    """I4：进入终态时句柄全部回收、上下文引用全部清空。"""
    table = make_table()
    resources = ResourceManager()
    table.bind_resources(resources)
    table.create("a1", quota=100)
    handle = resources.acquire("a1", HandleKind.LLM, frozenset({"EXEC"}), quota=100)
    table.get("a1").handles.add(handle.handle_id)
    table.transition("a1", AgentState.READY)
    table.transition("a1", AgentState.RUNNING)
    table.transition("a1", AgentState.TERMINATED)
    assert resources.get(handle.handle_id).state == "CLOSED"
    assert table.get("a1").handles == set()
    assert table.stats()["reclaimed_handles"] == 1
    # 终态后句柄不可再用（E_CLOSED）
    from runtime.handle import E_CLOSED

    with pytest.raises(RuntimeErr) as exc_info:
        resources.use(handle.handle_id, "a1", 1)
    assert exc_info.value.code == E_CLOSED


def test_failure_terminal_also_reclaims():
    """FAILED 同样是终态：句柄全部回收。"""
    table = make_table()
    resources = ResourceManager()
    table.bind_resources(resources)
    table.create("a1", quota=100)
    handle = resources.acquire("a1", HandleKind.TOOL, frozenset({"EXEC"}), quota=100)
    table.get("a1").handles.add(handle.handle_id)
    table.transition("a1", AgentState.READY)
    table.transition("a1", AgentState.FAILED)
    assert resources.get(handle.handle_id).state == "CLOSED"
    assert table.get("a1").handles == set()
