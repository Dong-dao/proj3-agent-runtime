"""resource.py 统一句柄单测：四种错误码逐个覆盖（契约① 第 3 节）。

验收：越权返回 E_PERM、超额返回 E_QUOTA、伪造或过期返回 E_BAD_HANDLE、
已关闭返回 E_CLOSED；release_all 终态全量回收。
"""

import pytest

from runtime.handle import (
    E_BAD_HANDLE,
    E_CLOSED,
    E_PERM,
    E_QUOTA,
    HandleKind,
    RuntimeErr,
)
from runtime.resource import FileArea, LLMStub, ResourceManager, ToolStub


def test_acquire_returns_open_handle_with_id():
    """acquire 发放 OPEN 句柄，handle_id 由 Runtime 统一发放（H 前缀递增）。"""
    mgr = ResourceManager()
    handle = mgr.acquire("a1", HandleKind.LLM, frozenset({"EXEC"}), quota=100)
    assert handle.handle_id.startswith("H")
    assert handle.state == "OPEN"
    assert handle.used == 0
    assert handle.refs == 1


def test_use_consumes_quota():
    """use 记账：used 累加且不超过 quota。"""
    mgr = ResourceManager()
    handle = mgr.acquire("a1", HandleKind.LLM, frozenset({"EXEC"}), quota=100)
    mgr.use(handle.handle_id, "a1", 60)
    mgr.use(handle.handle_id, "a1", 40)
    assert mgr.get(handle.handle_id).used == 100


def test_use_quota_exceeded():
    """超额返回 E_QUOTA。"""
    mgr = ResourceManager()
    handle = mgr.acquire("a1", HandleKind.LLM, frozenset({"EXEC"}), quota=100)
    mgr.use(handle.handle_id, "a1", 60)
    with pytest.raises(RuntimeErr) as exc_info:
        mgr.use(handle.handle_id, "a1", 41)
    assert exc_info.value.code == E_QUOTA


def test_use_forge_or_unknown_handle():
    """伪造或过期句柄返回 E_BAD_HANDLE。"""
    mgr = ResourceManager()
    with pytest.raises(RuntimeErr) as exc_info:
        mgr.use("H999999", "a1", 1)
    assert exc_info.value.code == E_BAD_HANDLE


def test_use_by_non_owner_is_perm():
    """越权（非 owner 操作）返回 E_PERM。"""
    mgr = ResourceManager()
    handle = mgr.acquire("a1", HandleKind.LLM, frozenset({"EXEC"}), quota=100)
    with pytest.raises(RuntimeErr) as exc_info:
        mgr.use(handle.handle_id, "a2", 1)
    assert exc_info.value.code == E_PERM


def test_use_without_perm_bit_is_perm():
    """缺权限位（FILE 句柄没有 READ）返回 E_PERM。"""
    mgr = ResourceManager()
    handle = mgr.acquire("a1", HandleKind.FILE, frozenset({"WRITE"}), quota=100)
    with pytest.raises(RuntimeErr) as exc_info:
        mgr.use(handle.handle_id, "a1", 1)  # use FILE 需要 READ
    assert exc_info.value.code == E_PERM


def test_use_closed_handle():
    """对已关闭句柄操作返回 E_CLOSED。"""
    mgr = ResourceManager()
    handle = mgr.acquire("a1", HandleKind.LLM, frozenset({"EXEC"}), quota=100)
    mgr.release(handle.handle_id, "a1")  # refs 1 -> 0，CLOSED
    with pytest.raises(RuntimeErr) as exc_info:
        mgr.use(handle.handle_id, "a1", 1)
    assert exc_info.value.code == E_CLOSED


def test_release_decrements_refs_zero_closes():
    """release：refs -= 1，归零才 CLOSED；重复释放返回 E_CLOSED。"""
    mgr = ResourceManager()
    handle = mgr.acquire("a1", HandleKind.LLM, frozenset({"EXEC"}), quota=100)
    mgr.release(handle.handle_id, "a1")  # refs 1 -> 0 -> CLOSED
    assert mgr.get(handle.handle_id).state == "CLOSED"
    with pytest.raises(RuntimeErr) as exc_info:
        mgr.release(handle.handle_id, "a1")
    assert exc_info.value.code == E_CLOSED


def test_release_by_non_owner_is_perm():
    mgr = ResourceManager()
    handle = mgr.acquire("a1", HandleKind.LLM, frozenset({"EXEC"}), quota=100)
    with pytest.raises(RuntimeErr) as exc_info:
        mgr.release(handle.handle_id, "a2")
    assert exc_info.value.code == E_PERM


def test_release_all_reclaims_everything():
    """release_all：回收该 Agent 全部句柄并返回数量（终态回收的执行体）。"""
    mgr = ResourceManager()
    h1 = mgr.acquire("a1", HandleKind.LLM, frozenset({"EXEC"}), quota=100)
    h2 = mgr.acquire("a1", HandleKind.TOOL, frozenset({"EXEC"}), quota=100)
    mgr.acquire("a2", HandleKind.FILE, frozenset({"READ"}), quota=100)
    reclaimed = mgr.release_all("a1")
    assert reclaimed == 2
    assert mgr.get(h1.handle_id).state == "CLOSED"
    assert mgr.get(h2.handle_id).state == "CLOSED"
    # 其他 Agent 的句柄不受影响（a2 的句柄仍是 OPEN）
    assert mgr.stats()["reclaim"] == 2
    others = [h for h in mgr._handles.values() if h.owner == "a2"]
    assert others and others[0].state == "OPEN"


def test_illegal_perms_rejected():
    """非法权限位集合：acquire 直接拒绝（E_PERM）。"""
    mgr = ResourceManager()
    with pytest.raises(RuntimeErr) as exc_info:
        mgr.acquire("a1", HandleKind.LLM, frozenset({"ADMIN"}), quota=100)
    assert exc_info.value.code == E_PERM


def test_stub_llm_deterministic():
    """LLM stub 决定论：同输入同输出，不接真实大模型。"""
    stub = LLMStub()
    assert stub.run("hello") == stub.run("hello")
    assert stub.run("hello") != stub.run("hello!")
    tool = ToolStub("search")
    assert tool.run("q") == tool.run("q")
