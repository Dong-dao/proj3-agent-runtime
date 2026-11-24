"""context.py：ContextStore —— S1 仅按契约①第 4 节签名提供 stub 实现。

本周规划红线：S1 不做 COW 上下文实体，S2 才实现「不可变前缀 + 引用计数 +
写入复制 + 域标签隔离」。本文件只冻结签名、维持 refs 记账与跨域读取校验的
最小形态，不改契约签名。

不变量（S2 完整实现，S1 stub 先保证的）：
- refs 恒等于持有者数量；归零才释放；
- 跨隔离域读取被拒绝（S1 单域，域校验骨架就位）。

日志（产生）：acquire / read / write / release 各记一条 INFO 日志。
计数：stats() 返回 copy_count / copy_bytes / share_hit_rate（S1 恒为 0）。
"""

from __future__ import annotations

import logging

log = logging.getLogger("runtime.context")


class ContextStore:
    """上下文存储 stub：S1 冻结签名，S2 实现 COW 语义（字段可加，语义不可改）。"""

    def __init__(self) -> None:
        self._objects: dict[str, dict] = {}      # ctx_id -> {bytes, domain_tag, refs, holders}
        self._refs_of: dict[str, set[str]] = {}  # agent_id -> 持有的 ctx_id
        self._seq = 0

    def acquire(self, agent_id: str, prefix_id: str) -> str:
        """获取共享上下文引用：refs += 1，登记持有者，返回 ctx_id。

        维护：refs 恒等于持有者数量；产生：acquire 计数日志。
        """
        self._seq += 1
        ctx_id = f"C{self._seq:06d}"
        self._objects[ctx_id] = {
            "bytes": prefix_id.encode("utf-8"),
            "domain_tag": agent_id,
            "refs": 1,
            "holders": {agent_id},
        }
        self._refs_of.setdefault(agent_id, set()).add(ctx_id)
        log.info("acquire %s by %s (prefix=%s)", ctx_id, agent_id, prefix_id)
        return ctx_id

    def read(self, agent_id: str, ctx_id: str) -> bytes:
        """读取上下文内容：S1 单域，跨域校验骨架就位（越权抛 E_PERM）。

        维护：域校验；产生：域校验日志。
        """
        obj = self._objects.get(ctx_id)
        if obj is None or agent_id not in obj["holders"]:
            log.warning("E_PERM read %s by %s: 未持有或不存在", ctx_id, agent_id)
            from .handle import RuntimeErr, E_PERM

            raise RuntimeErr(E_PERM, f"{agent_id} 无权读取 {ctx_id}")
        log.info("read %s by %s", ctx_id, agent_id)
        return obj["bytes"]

    def write(self, agent_id: str, ctx_id: str, data: bytes) -> str:
        """写入上下文：S1 stub 直接覆盖并返回新 ctx_id；COW 复制计数 S2 实现。

        维护：S2 起仅当 refs > 1 才复制；产生：copy_count / copy_bytes 计数日志。
        """
        obj = self._objects.get(ctx_id)
        if obj is None or agent_id not in obj["holders"]:
            from .handle import RuntimeErr, E_PERM

            log.warning("E_PERM write %s by %s: 未持有或不存在", ctx_id, agent_id)
            raise RuntimeErr(E_PERM, f"{agent_id} 无权写入 {ctx_id}")
        self._seq += 1
        new_id = f"C{self._seq:06d}"
        self._objects[new_id] = {
            "bytes": data,
            "domain_tag": agent_id,
            "refs": 1,
            "holders": {agent_id},
        }
        self._release_ref(agent_id, ctx_id)
        self._refs_of.setdefault(agent_id, set()).add(new_id)
        # S1 stub：copy_count / copy_bytes 不计数，S2 COW 实现后启用
        log.info("write %s -> %s by %s (S1 stub, 无 COW 计数)", ctx_id, new_id, agent_id)
        return new_id

    def release(self, agent_id: str, ctx_id: str) -> None:
        """释放持有：refs -= 1，归零才释放。

        维护：refs 归零才释放；产生：release 计数日志。
        """
        self._release_ref(agent_id, ctx_id)

    def _release_ref(self, agent_id: str, ctx_id: str) -> None:
        obj = self._objects.get(ctx_id)
        if obj is None or agent_id not in obj["holders"]:
            from .handle import RuntimeErr, E_BAD_HANDLE

            log.warning("E_BAD_HANDLE release %s by %s", ctx_id, agent_id)
            raise RuntimeErr(E_BAD_HANDLE, f"{agent_id} 未持有 {ctx_id}")
        obj["refs"] -= 1
        obj["holders"].discard(agent_id)
        self._refs_of.get(agent_id, set()).discard(ctx_id)
        if obj["refs"] <= 0:
            self._objects.pop(ctx_id, None)
            log.info("release %s by %s: refs=0，对象已释放", ctx_id, agent_id)
        else:
            log.info("release %s by %s: refs=%d", ctx_id, agent_id, obj["refs"])

    def stats(self) -> dict:
        """返回 copy_count / copy_bytes / share_hit_rate 当前值（S1 恒为 0）。"""
        return {"copy_count": 0, "copy_bytes": 0, "share_hit_rate": 0.0}
