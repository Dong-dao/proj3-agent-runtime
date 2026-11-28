"""context.py：ContextStore —— COW 上下文（S2 机制①，S2-01 设计说明冻结口径）。

v1（11/24）：不可变共享前缀 + 引用计数。
v2（11/25）：分支写入触发复制，copy_count / copy_bytes 计数。

语义（S2-01 第 2/3 节，写入判定点唯一：write() 的复制分支）：
- acquire(agent_id, prefix_id)：登记共享前缀对象；同一 prefix_id 的后续
  acquire 加入同一对象（显式登记后的共享），refs +1、holders 登记持有者；
- write(agent_id, ctx_id, data)：refs > 1 → 复制分支，copy_count += 1、
  copy_bytes += len(data)；refs == 1 → 独占分支，不增拷贝计数
  （统一构造新对象，语义等价于「无共享则无额外拷贝代价」）；
- release(agent_id, ctx_id)：refs -= 1，归零才从对象表移除。

不变量（S2-01 第 4 节）：
- C1 refs == len(holders) 恒成立（acquire / _release_ref 成对修改两个结构）；
- C2 refs > 1 写入必复制，计数 +1；
- C3 refs == 1 写入不增拷贝计数；
- C4 refs == 0 才释放对象；
- C5 非持有者读写被拒（E_PERM）。

OS 出处：fork() 写时复制——迁移「不可变共享 + 写入才复制」思想，
是逻辑 COW，不是页表级实现（答辩边界声明见 S2-01 第 7 节）。

日志（产生）：acquire / read / write / release 各记一条 INFO；
复制分支记一条带 copy_count 的 INFO；越权拒绝记带 E_PERM 的 WARNING。
"""

from __future__ import annotations

import logging

from .handle import E_BAD_HANDLE, E_PERM, RuntimeErr

log = logging.getLogger("runtime.context")


class ContextStore:
    """COW 上下文存储：共享前缀 + 引用计数 + 分支写入触发复制。

    契约①第 4 节签名不变；S2 只填内部逻辑（S2-01 第 6 节对接点）。
    """

    def __init__(self) -> None:
        self._objects: dict[str, dict] = {}      # ctx_id -> {bytes, domain_tag, refs, holders}
        self._refs_of: dict[str, set[str]] = {}  # agent_id -> 持有的 ctx_id 集合
        self._prefix_index: dict[str, str] = {}  # prefix_id -> 现存共享对象的 ctx_id
        self._seq = 0
        # 计数（契约② 字段：copy_count / copy_bytes / share_hit_rate）
        self._copy_count = 0
        self._copy_bytes = 0
        self._acquire_calls = 0
        self._share_hits = 0

    # ---- 登记与共享 ----------------------------------------------------------
    def acquire(self, agent_id: str, prefix_id: str) -> str:
        """获取共享上下文引用：refs += 1，登记持有者，返回 ctx_id。

        同一 prefix_id 已有存活对象时加入该对象（共享命中，share_hit_rate 的分子）；
        否则新建对象。维护 C1：refs 恒等于持有者数量。
        """
        self._acquire_calls += 1
        existing = self._prefix_index.get(prefix_id)
        if existing is not None and existing in self._objects:
            obj = self._objects[existing]
            obj["refs"] += 1
            obj["holders"].add(agent_id)
            self._refs_of.setdefault(agent_id, set()).add(existing)
            self._share_hits += 1
            log.info(
                "acquire %s by %s (prefix=%s, 共享命中 refs=%d)",
                existing, agent_id, prefix_id, obj["refs"],
            )
            return existing
        self._seq += 1
        ctx_id = f"C{self._seq:06d}"
        self._objects[ctx_id] = {
            "bytes": prefix_id.encode("utf-8"),
            "domain_tag": agent_id,
            "refs": 1,
            "holders": {agent_id},
        }
        self._prefix_index[prefix_id] = ctx_id
        self._refs_of.setdefault(agent_id, set()).add(ctx_id)
        log.info("acquire %s by %s (prefix=%s, 新建对象)", ctx_id, agent_id, prefix_id)
        return ctx_id

    # ---- 读写 ----------------------------------------------------------------
    def read(self, agent_id: str, ctx_id: str) -> bytes:
        """读取上下文内容：非持有者拒绝（C5，越权抛 E_PERM 并留日志）。"""
        obj = self._objects.get(ctx_id)
        if obj is None or agent_id not in obj["holders"]:
            log.warning("E_PERM read %s by %s: 未持有或不存在", ctx_id, agent_id)
            raise RuntimeErr(E_PERM, f"{agent_id} 无权读取 {ctx_id}")
        log.info("read %s by %s", ctx_id, agent_id)
        return obj["bytes"]

    def write(self, agent_id: str, ctx_id: str, data: bytes) -> str:
        """写入上下文：v2 COW——仅当 refs > 1 才复制并计数（判定点唯一）。

        复制分支（C2）：copy_count += 1、copy_bytes += len(data)，调用方引用
        迁移到新对象，旧对象 refs -1；独占分支（C3）：构造新对象但不增拷贝计数。
        """
        obj = self._objects.get(ctx_id)
        if obj is None or agent_id not in obj["holders"]:
            log.warning("E_PERM write %s by %s: 未持有或不存在", ctx_id, agent_id)
            raise RuntimeErr(E_PERM, f"{agent_id} 无权写入 {ctx_id}")
        data = bytes(data)
        copied = obj["refs"] > 1
        self._seq += 1
        new_id = f"C{self._seq:06d}"
        self._objects[new_id] = {
            "bytes": data,
            "domain_tag": obj["domain_tag"],
            "refs": 1,
            "holders": {agent_id},
        }
        self._release_ref(agent_id, ctx_id)
        self._refs_of.setdefault(agent_id, set()).add(new_id)
        if copied:
            self._copy_count += 1
            self._copy_bytes += len(data)
            log.info(
                "write %s -> %s by %s: COW 复制分支 copy_count=%d copy_bytes=%d",
                ctx_id, new_id, agent_id, self._copy_count, self._copy_bytes,
            )
        else:
            log.info(
                "write %s -> %s by %s: 独占分支（refs==1，不复制不计数）",
                ctx_id, new_id, agent_id,
            )
        return new_id

    # ---- 释放 ----------------------------------------------------------------
    def release(self, agent_id: str, ctx_id: str) -> None:
        """释放持有：refs -= 1，归零才释放（C4）。"""
        self._release_ref(agent_id, ctx_id)

    def _release_ref(self, agent_id: str, ctx_id: str) -> None:
        obj = self._objects.get(ctx_id)
        if obj is None or agent_id not in obj["holders"]:
            log.warning("E_BAD_HANDLE release %s by %s", ctx_id, agent_id)
            raise RuntimeErr(E_BAD_HANDLE, f"{agent_id} 未持有 {ctx_id}")
        obj["refs"] -= 1
        obj["holders"].discard(agent_id)
        self._refs_of.get(agent_id, set()).discard(ctx_id)
        if obj["refs"] <= 0:
            self._objects.pop(ctx_id, None)
            for pid, cid in list(self._prefix_index.items()):
                if cid == ctx_id:
                    self._prefix_index.pop(pid, None)
            log.info("release %s by %s: refs=0，对象已释放", ctx_id, agent_id)
        else:
            log.info("release %s by %s: refs=%d", ctx_id, agent_id, obj["refs"])

    # ---- 计数导出 --------------------------------------------------------------
    def stats(self) -> dict:
        """契约② 字段：copy_count / copy_bytes / share_hit_rate 当前值。"""
        share_hit_rate = (
            self._share_hits / self._acquire_calls if self._acquire_calls else 0.0
        )
        return {
            "copy_count": self._copy_count,
            "copy_bytes": self._copy_bytes,
            "share_hit_rate": round(share_hit_rate, 4),
        }

    # ---- 观测辅助（只读，供 Observer / 测试用） ----------------------------------
    def object_count(self) -> int:
        """存活上下文对象数（只读诊断）。"""
        return len(self._objects)
