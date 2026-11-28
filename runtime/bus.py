"""bus.py：本地消息总线与共享上下文区（S2 机制④，S2-04 设计说明冻结口径）。

机制：
- 消息结构：msgid / sender / receiver / ts / ctx_ref（payload_ref）——
  ctx_ref 指向共享上下文区的 ctx_id，不是数据副本（引用传递）；
- 共享上下文区：复用 ContextStore 的对象；投递时经 grant 完成「接收方
  acquire」语义（refs +1），用完 release，引用生命周期成对（M5）；
- 不丢不重：msgid 全局递增唯一（M1）；队列 + delivered 集合（M2/M3），
  重复投递被拒并计数。

为什么不做序列化拷贝（必答项，S2-04 第 3 节）：
每条消息 pickle/JSON 序列化整份上下文 → 通信字节数线性放大、拷贝次数与
消息数正相关，COW 省下的拷贝全部吐回去；共享区 + 引用传递 → 消息只带
ctx_ref，多个接收方共享同一对象，靠 refs 管理生命周期，拷贝只在写入时发生。
消息总线与 COW 上下文是同一条优化链：总线传引用，COW 写入才复制。

OS 出处：共享内存 + 消息队列（System V / POSIX IPC）——发送方把数据放进
共享区，接收方按引用读取。是进程内逻辑总线，不是内核 IPC，不做跨机通信。

日志（产生）：send / recv 各记一条 INFO；重复投递拒绝记 WARNING。
计数：message_count（发送条数）/ comm_bytes（通信字节数，元数据），
经 stats() 导出（契约② 字段名冻结）。
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass

from .handle import E_BAD_HANDLE, E_PERM, RuntimeErr

log = logging.getLogger("runtime.bus")


@dataclass(frozen=True)
class Message:
    """消息实体：payload 只存共享上下文区的引用（M4：无字节副本）。"""

    msg_id: str      # 全局递增唯一（M1）
    sender: str      # 发送方 agent_id
    receiver: str    # 接收方 agent_id
    ts: float        # 发送时刻（wall clock，秒）
    ctx_ref: str     # payload_ref：指向 ContextStore 的 ctx_id（引用，非副本）


class MessageBus:
    """进程内本地消息总线：FIFO 队列 + 共享上下文区引用传递。"""

    def __init__(self, context_store=None) -> None:
        self._context = context_store
        self._queues: dict[str, deque[Message]] = {}   # receiver -> 消息队列
        self._delivered: set[str] = set()              # 已投递 msgid（M3 去重）
        self._seq = 0
        self._message_count = 0     # 发送条数（契约②：发 N 条 = N）
        self._recv_count = 0        # 实际投递条数
        self._comm_bytes = 0        # 通信字节数（仅消息元数据，不含 payload 副本）
        self._dup_rejected = 0      # 重复投递拒绝次数

    # ---- 发送 ----------------------------------------------------------------
    def send(self, sender: str, receiver: str, ctx_ref: str) -> str:
        """发送消息：只登记 ctx_ref 引用，不拷贝数据（M4）。

        返回 msgid。comm_bytes 只累计消息头元数据（msgid/sender/receiver/时间戳），
        与序列化整份上下文的方案形成可测的量级对比。
        """
        self._seq += 1
        msg_id = f"M{self._seq:08d}"
        meta = (
            len(msg_id.encode("utf-8"))
            + len(sender.encode("utf-8"))
            + len(receiver.encode("utf-8"))
            + len(ctx_ref.encode("utf-8"))
            + 8  # ts: float64
        )
        msg = Message(msg_id, sender, receiver, time.time(), ctx_ref)
        self._queues.setdefault(receiver, deque()).append(msg)
        self._message_count += 1
        self._comm_bytes += meta
        log.info(
            "send %s: %s -> %s (ctx_ref=%s, 元数据 %dB, 引用传递无 payload 副本)",
            msg_id, sender, receiver, ctx_ref, meta,
        )
        return msg_id

    # ---- 接收 ----------------------------------------------------------------
    def recv(self, receiver: str) -> Message | None:
        """取队首消息并标记已投递；经 grant 完成「接收方 acquire」（M5）。

        不丢（M2）：队列非空必取到；不重（M3）：投递即从队列摘除并记入
        delivered，同一 msgid 二次投递被拒并计数。队列为空返回 None。
        """
        q = self._queues.get(receiver)
        while q:
            msg = q.popleft()
            if msg.msg_id in self._delivered:
                # 防御分支：结构性上不可达（投递即摘除），保留以兑现 M3 承诺
                self._dup_rejected += 1
                log.warning(
                    "E_DUP %s: 重复投递被拒（receiver=%s，拒绝 #%d）",
                    msg.msg_id, receiver, self._dup_rejected,
                )
                continue
            self._delivered.add(msg.msg_id)
            self._recv_count += 1
            if self._context is not None:
                # 接收方 acquire 语义：显式登记共享（refs +1），与 release 成对（M5）；
                # 已持有（E_BAD_HANDLE）幂等跳过——同一 ctx 多条消息不重复计数 refs；
                # 跨域共享被拒（E_PERM）只拒登记不拒投递：消息按引用送达，
                # 接收方后续 read 仍会被隔离域校验拦下（未登记即隔离）
                try:
                    self._context.grant(msg.sender, msg.ctx_ref, receiver)
                except RuntimeErr as exc:
                    if exc.code not in (E_BAD_HANDLE, E_PERM):
                        raise
                    log.info(
                        "recv %s: %s 对 %s 的共享登记被拒（%s），消息按引用送达",
                        msg.msg_id, receiver, msg.ctx_ref, exc.code,
                    )
            log.info(
                "recv %s by %s (sender=%s, ctx_ref=%s)",
                msg.msg_id, receiver, msg.sender, msg.ctx_ref,
            )
            return msg
        return None

    def assert_not_delivered(self, msg_id: str) -> None:
        """显式去重校验：msgid 已投递则抛 E_BAD_HANDLE（M3 可直接触发）。"""
        if msg_id in self._delivered:
            self._dup_rejected += 1
            log.warning(
                "E_DUP %s: 已投递消息不允许再次投递（拒绝 #%d）",
                msg_id, self._dup_rejected,
            )
            raise RuntimeErr(E_BAD_HANDLE, f"消息 {msg_id} 已投递，拒绝重复投递")

    # ---- 计数导出 --------------------------------------------------------------
    def stats(self) -> dict:
        """契约② 字段：message_count / comm_bytes（+ 诊断量）。

        comm_bytes 在引用传递下只含元数据，量级远小于序列化方案
        （S2-04 第 5 节可数证据）。
        """
        queued = sum(len(q) for q in self._queues.values())
        return {
            "message_count": self._message_count,
            "comm_bytes": self._comm_bytes,
            "recv_count": self._recv_count,
            "dup_rejected": self._dup_rejected,
            "queued": queued,
        }
