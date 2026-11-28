"""test_bus.py：本地消息总线与共享上下文区（S2-04 不变量逐条）。"""

import pytest

from runtime.bus import MessageBus
from runtime.context import ContextStore
from runtime.handle import E_BAD_HANDLE, RuntimeErr


def test_m1_msgid_unique():
    """M1：发 N 条消息，msgid 全局递增唯一。"""
    bus = MessageBus()
    mids = [bus.send("A", "B", "C000001") for _ in range(10)]
    assert len(set(mids)) == 10
    assert len(mids) == bus.stats()["message_count"]


def test_m2_no_message_lost():
    """M2：发 N 收 N，全部收到且内容（引用）一致。"""
    cs = ContextStore()
    cs.set_domain("A", "D"); cs.set_domain("B", "D")
    bus = MessageBus(context_store=cs)
    ctx = cs.acquire("A", "payload-prefix")
    mids = [bus.send("A", "B", ctx) for _ in range(5)]
    got = [bus.recv("B") for _ in range(5)]
    assert all(m is not None for m in got)
    assert [m.msg_id for m in got] == mids  # FIFO 队首顺序
    assert all(m.ctx_ref == ctx for m in got)
    assert bus.recv("B") is None  # 队列已空


def test_m3_no_duplicate_delivery():
    """M3：重复投递被拒并计数；已投递 msgid 再次投递抛 E_BAD_HANDLE。"""
    cs = ContextStore()
    cs.set_domain("A", "D"); cs.set_domain("B", "D")
    bus = MessageBus(context_store=cs)
    ctx = cs.acquire("A", "P")
    mid = bus.send("A", "B", ctx)
    msg = bus.recv("B")
    assert msg is not None and msg.msg_id == mid
    assert bus.recv("B") is None  # 已投递即摘除，队列空
    with pytest.raises(RuntimeErr) as exc:
        bus.assert_not_delivered(mid)
    assert exc.value.code == E_BAD_HANDLE
    assert bus.stats()["dup_rejected"] == 1


def test_m4_no_serialized_copy_on_send():
    """M4：发消息不产生字节副本——copy_count 不因发消息增加，消息体无 payload。"""
    cs = ContextStore()
    cs.set_domain("A", "D"); cs.set_domain("B", "D")
    bus = MessageBus(context_store=cs)
    ctx = cs.acquire("A", "payload-prefix")
    copies_before = cs.stats()["copy_count"]
    for _ in range(10):
        bus.send("A", "B", ctx)
    msg = bus.recv("B")
    assert msg.ctx_ref == ctx          # 只带引用
    assert not hasattr(msg, "payload")  # 消息结构无数据副本字段
    assert cs.stats()["copy_count"] == copies_before


def test_m5_grant_release_paired_lifecycle():
    """M5：投递经 grant 完成接收方 acquire（refs+1），release 后成对减回。"""
    cs = ContextStore()
    cs.set_domain("A", "D"); cs.set_domain("B", "D")
    bus = MessageBus(context_store=cs)
    ctx = cs.acquire("A", "P")
    assert cs._objects[ctx]["refs"] == 1
    bus.send("A", "B", ctx)
    bus.recv("B")
    obj = cs._objects[ctx]
    assert obj["refs"] == 2 == len(obj["holders"])  # 接收方 acquire 语义
    cs.release("B", ctx)
    obj = cs._objects[ctx]
    assert obj["refs"] == 1 == len(obj["holders"])  # 成对释放


def test_comm_bytes_metadata_only():
    """comm_bytes 只含消息头元数据（引用传递），量级远小于序列化方案。"""
    cs = ContextStore()
    cs.set_domain("A", "D"); cs.set_domain("B", "D")
    bus = MessageBus(context_store=cs)
    ctx = cs.acquire("A", "P")
    n = 8
    for _ in range(n):
        bus.send("A", "B", ctx)
    stats = bus.stats()
    assert stats["comm_bytes"] == 26 * n  # 每条 26B 元数据（9+1+1+7+8）
    # 对照：若序列化 payload（哪怕只有前缀 13 字节），每条至少翻倍
    assert stats["comm_bytes"] < n * len(ctx.encode() + b"payload-prefix" * 4)


def test_cross_domain_grant_rejected_but_message_delivered():
    """隔离域边界：跨域投递时 grant 被拒，消息本身仍按引用投递（不丢）。"""
    cs = ContextStore()
    cs.set_domain("A", "D1")
    cs.set_domain("C", "D2")
    bus = MessageBus(context_store=cs)
    ctx = cs.acquire("A", "P")
    bus.send("A", "C", ctx)
    msg = bus.recv("C")  # grant 拒绝不应吞掉消息
    assert msg is not None
    assert "C" not in cs._objects[ctx]["holders"]  # 越权共享未发生
    with pytest.raises(RuntimeErr):
        cs.read("C", ctx)


def test_recv_empty_queue_returns_none():
    """队列为空时 recv 返回 None，不抛异常。"""
    bus = MessageBus()
    assert bus.recv("nobody") is None
