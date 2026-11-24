"""bus.py：本地消息总线 —— S1 仅骨架，S2 实现（本周规划红线：本周不做消息总线）。

S2 要实现的机制（设计文档 3.2.5）：
- 本地队列 + 共享上下文区，消息带 msgid / sender / receiver / ts；
- 不变量：消息可追踪、不丢不重；不做序列化拷贝、不做跨机通信；
- 计数：消息条数、通信字节数。
"""

from __future__ import annotations

import logging

log = logging.getLogger("runtime.bus")


class MessageBus:
    """进程内消息总线骨架：S2 实现（S1 不实例化）。"""

    def __init__(self) -> None:
        self._sent = 0

    def stats(self) -> dict:
        """S2 计数：消息条数 message_count、通信字节数 comm_bytes。"""
        return {"message_count": self._sent, "comm_bytes": 0}
