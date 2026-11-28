"""test_context.py：COW 上下文 + 隔离域 + 裁剪（S2-01/S2-02 不变量逐条）。

覆盖本周规划第四节步骤 3 的三组单测中的两组：
- 共享计数：两 Agent 共享同一前缀，refs == 2、share_hit_rate > 0；
- COW 复制：refs > 1 写入后 copy_count +1、copy_bytes 增长；
- 越权拒绝：非持有者 / 跨域读写被拒并留计数。
"""

import pytest

from runtime.context import ContextStore
from runtime.handle import E_BAD_HANDLE, E_PERM, RuntimeErr


def test_two_agents_share_prefix_refs_is_2():
    """11/24 验收：两 Agent 共享同一前缀，同一对象、refs == 2、命中率 > 0。"""
    store = ContextStore()
    store.set_domain("agent-A", "D1")
    store.set_domain("agent-B", "D1")
    ctx_a = store.acquire("agent-A", "SYS-PROMPT-v1")
    ctx_b = store.acquire("agent-B", "SYS-PROMPT-v1")
    assert ctx_a == ctx_b, "同 prefix_id 必须命中同一共享对象"
    obj = store._objects[ctx_a]
    assert obj["refs"] == 2
    assert obj["holders"] == {"agent-A", "agent-B"}
    assert obj["refs"] == len(obj["holders"])  # C1
    stats = store.stats()
    assert stats["share_hit_rate"] > 0
    assert stats["copy_count"] == 0  # 只共享未写入，无拷贝


def test_refs_constant_after_acquire_release_cycles():
    """C1：多次 acquire/release 后 refs == len(holders) 恒成立；归零对象移除（C4）。"""
    store = ContextStore()
    store.set_domain("a", "D"); store.set_domain("b", "D"); store.set_domain("c", "D")
    ctx = store.acquire("a", "P")
    store.acquire("b", "P")
    store.acquire("c", "P")
    obj = store._objects[ctx]
    assert obj["refs"] == 3 == len(obj["holders"])
    store.release("b", ctx)
    assert store._objects[ctx]["refs"] == 2 == len(store._objects[ctx]["holders"])
    store.release("a", ctx)
    store.release("c", ctx)
    assert ctx not in store._objects  # C4


def test_cow_copy_on_shared_write():
    """C2：refs > 1 时写入必复制，copy_count 0->1、copy_bytes 增 len(data)。"""
    store = ContextStore()
    store.set_domain("agent-A", "D1")
    store.set_domain("agent-B", "D1")
    ctx = store.acquire("agent-A", "P")
    store.acquire("agent-B", "P")
    data = b"agent-A-branch-data"
    new_ctx = store.write("agent-A", ctx, data)
    stats = store.stats()
    assert stats["copy_count"] == 1
    assert stats["copy_bytes"] == len(data)
    # 调用方引用迁移到新对象，旧对象仍被 B 持有
    assert store.read("agent-A", new_ctx) == data
    assert store.read("agent-B", ctx) == b"P"
    obj = store._objects[ctx]
    assert obj["holders"] == {"agent-B"} and obj["refs"] == 1  # C1 仍成立


def test_exclusive_write_does_not_copy():
    """C3：refs == 1 独占写入不增加拷贝计数。"""
    store = ContextStore()
    store.set_domain("solo", "D")
    ctx = store.acquire("solo", "P")
    store.write("solo", ctx, b"private-data")
    stats = store.stats()
    assert stats["copy_count"] == 0
    assert stats["copy_bytes"] == 0


def test_unauthorized_read_write_rejected():
    """C5/D1：非持有者读写被拒，E_PERM 留日志且 perm_denied 计数增长。"""
    store = ContextStore()
    store.set_domain("owner", "D1")
    store.set_domain("intruder", "D2")
    ctx = store.acquire("owner", "P")
    denied_before = store.stats()["perm_denied"]
    with pytest.raises(RuntimeErr) as exc:
        store.read("intruder", ctx)
    assert exc.value.code == E_PERM
    with pytest.raises(RuntimeErr) as exc:
        store.write("intruder", ctx, b"x")
    assert exc.value.code == E_PERM
    assert store.stats()["perm_denied"] == denied_before + 2


def test_cross_domain_acquire_read_rejected():
    """D1 隔离域：跨域 acquire / read 被拒；同域显式 acquire 才共享。"""
    store = ContextStore()
    store.set_domain("a1", "D1")
    store.set_domain("a2", "D1")
    store.set_domain("b1", "D2")
    ctx = store.acquire("a1", "P")
    assert store.acquire("a2", "P") == ctx  # 同域显式登记 -> 共享
    with pytest.raises(RuntimeErr) as exc:
        store.acquire("b1", "P")  # 跨域加入共享被拒
    assert exc.value.code == E_PERM
    with pytest.raises(RuntimeErr) as exc:
        store.read("b1", ctx)
    assert exc.value.code == E_PERM


def test_grant_explicit_share_same_domain_only():
    """显式 grant：同域登记成功（refs+1）；重复登记与跨域被拒。"""
    store = ContextStore()
    store.set_domain("a1", "D1")
    store.set_domain("a2", "D1")
    store.set_domain("b1", "D2")
    ctx = store.acquire("a1", "P")
    store.grant("a1", ctx, "a2")
    obj = store._objects[ctx]
    assert obj["refs"] == 2 == len(obj["holders"])
    with pytest.raises(RuntimeErr) as exc:
        store.grant("a1", ctx, "a2")  # 重复登记破坏 C1
    assert exc.value.code == E_BAD_HANDLE
    with pytest.raises(RuntimeErr) as exc:
        store.grant("a1", ctx, "b1")  # 跨域共享被拒
    assert exc.value.code == E_PERM
    assert obj["refs"] == 2  # 拒绝后计数不变


def test_trim_enforces_threshold_and_keeps_refs():
    """D3/D4：裁剪后长度 <= 阈值；只改 bytes 不改 holders/refs。"""
    store = ContextStore(max_context_bytes=16)
    store.set_domain("a", "D")
    ctx = store.acquire("a", "P")
    long_data = b"x" * 40
    new_ctx = store.write("a", ctx, long_data)
    content = store.read("a", new_ctx)
    assert len(content) <= 16
    assert content == long_data[-16:]  # 保留最近内容
    assert store.stats()["trim_count"] == 1
    obj = store._objects[new_ctx]
    assert obj["refs"] == 1 == len(obj["holders"])  # D4


def test_release_unknown_context_rejected():
    """释放未持有对象被拒（E_BAD_HANDLE）。"""
    store = ContextStore()
    store.set_domain("a", "D")
    ctx = store.acquire("a", "P")
    with pytest.raises(RuntimeErr) as exc:
        store.release("other", ctx)
    assert exc.value.code == E_BAD_HANDLE
