"""context.py：ContextStore —— COW 上下文（S2 机制①，S2-01 设计说明冻结口径）。

v1（11/24）：不可变共享前缀 + 引用计数。
v2（11/25）：分支写入触发复制，copy_count / copy_bytes 计数。
隔离域（11/26）：域标签 + holders 双重校验，越权直接拒绝（S2-02 冻结口径）；
共享必须显式登记（acquire / grant），未登记即隔离。裁剪（11/26）：
超过阈值按定长裁剪（保留最近 N 字节），不做语义压缩、不引入推理框架。

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
- C5 非持有者读写被拒（E_PERM）；
- D1 非持有者/跨域读写被拒（域标签 + holders 双重判定，E_PERM）；
- D3 裁剪后上下文长度不超过阈值；
- D4 裁剪只改 bytes，不改 holders / refs（refs == len(holders) 仍成立）。

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

    def __init__(self, max_context_bytes: int = 4096) -> None:
        self._objects: dict[str, dict] = {}      # ctx_id -> {bytes, domain_tag, refs, holders}
        self._refs_of: dict[str, set[str]] = {}  # agent_id -> 持有的 ctx_id 集合
        self._prefix_index: dict[str, str] = {}  # prefix_id -> 现存共享对象的 ctx_id
        self._agent_domain: dict[str, str] = {}  # agent_id -> 域标签（缺省域 = agent_id）
        self._max_context_bytes = max(1, int(max_context_bytes))  # 裁剪阈值（定长）
        self._seq = 0
        # 计数（契约② 字段：copy_count / copy_bytes / share_hit_rate）
        self._copy_count = 0
        self._copy_bytes = 0
        self._acquire_calls = 0
        self._share_hits = 0
        self._perm_denied = 0   # 越权拒绝次数（S2-02 可数证据）
        self._trim_count = 0    # 裁剪次数

    # ---- 隔离域（S2-02） ------------------------------------------------------
    def set_domain(self, agent_id: str, domain: str) -> None:
        """声明 Agent 所属隔离域；未声明的 Agent 缺省属于以自身 id 命名的域。

        域边界（S2-02 第 2 节）：以 Agent 为最小隔离单位；共享前缀是
        「显式 acquire 后的共享」，不是「默认全域可读」。
        """
        self._agent_domain[agent_id] = domain
        log.debug("context: agent %s joined domain %s", agent_id, domain)

    def _domain_of(self, agent_id: str) -> str:
        return self._agent_domain.get(agent_id, agent_id)

    def _check_access(self, agent_id: str, ctx_id: str, op: str,
                      require_holder: bool = True) -> dict:
        """域标签 + holders 双重校验（D1）：任一不满足即 E_PERM 并留日志。

        acquire 加入共享是「成为持有者」的动作本身，只查域标签（require_holder=False）；
        read / write / grant 必须已是持有者，再叠加域标签判定。
        """
        obj = self._objects.get(ctx_id)
        if obj is not None and require_holder and agent_id not in obj["holders"]:
            self._perm_denied += 1
            log.warning(
                "E_PERM %s %s by %s: 未持有或不存在（越权拒绝 #%d）",
                op, ctx_id, agent_id, self._perm_denied,
            )
            raise RuntimeErr(E_PERM, f"{agent_id} 无权{op} {ctx_id}")
        if obj is None:
            self._perm_denied += 1
            log.warning(
                "E_PERM %s %s by %s: 不存在（越权拒绝 #%d）",
                op, ctx_id, agent_id, self._perm_denied,
            )
            raise RuntimeErr(E_PERM, f"上下文 {ctx_id} 不存在，{agent_id} 无法{op}")
        if self._domain_of(agent_id) != obj["domain_tag"]:
            self._perm_denied += 1
            log.warning(
                "E_PERM %s %s by %s: 域标签不匹配（agent 域=%s, 对象域=%s，越权拒绝 #%d）",
                op, ctx_id, agent_id, self._domain_of(agent_id),
                obj["domain_tag"], self._perm_denied,
            )
            raise RuntimeErr(
                E_PERM,
                f"{agent_id}（域 {self._domain_of(agent_id)}）跨域{op} {ctx_id}"
                f"（域 {obj['domain_tag']}）被拒绝",
            )
        return obj

    # ---- 登记与共享 ----------------------------------------------------------
    def acquire(self, agent_id: str, prefix_id: str) -> str:
        """获取共享上下文引用：refs += 1，登记持有者，返回 ctx_id。

        同一 prefix_id 已有存活对象时加入该对象（共享命中，share_hit_rate 的分子）；
        否则新建对象。维护 C1：refs 恒等于持有者数量。
        """
        self._acquire_calls += 1
        existing = self._prefix_index.get(prefix_id)
        if existing is not None and existing in self._objects:
            obj = self._check_access(agent_id, existing, "acquire", require_holder=False)
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
        content = prefix_id.encode("utf-8")
        self._objects[ctx_id] = {
            "bytes": self._trim(content, ctx_id),
            "domain_tag": self._domain_of(agent_id),
            "refs": 1,
            "holders": {agent_id},
        }
        self._prefix_index[prefix_id] = ctx_id
        self._refs_of.setdefault(agent_id, set()).add(ctx_id)
        log.info("acquire %s by %s (prefix=%s, 新建对象)", ctx_id, agent_id, prefix_id)
        return ctx_id

    # ---- 读写 ----------------------------------------------------------------
    def read(self, agent_id: str, ctx_id: str) -> bytes:
        """读取上下文内容：域标签 + holders 双重校验，越权抛 E_PERM 并留日志。"""
        obj = self._check_access(agent_id, ctx_id, "read")
        log.info("read %s by %s", ctx_id, agent_id)
        return obj["bytes"]

    def write(self, agent_id: str, ctx_id: str, data: bytes) -> str:
        """写入上下文：v2 COW——仅当 refs > 1 才复制并计数（判定点唯一）。

        复制分支（C2）：copy_count += 1、copy_bytes += len(data)，调用方引用
        迁移到新对象，旧对象 refs -1；独占分支（C3）：构造新对象但不增拷贝计数。
        """
        obj = self._check_access(agent_id, ctx_id, "write")
        data = bytes(data)
        copied = obj["refs"] > 1
        self._seq += 1
        new_id = f"C{self._seq:06d}"
        content = self._trim(data, new_id)  # 裁剪在复制之前定长完成
        self._objects[new_id] = {
            "bytes": content,
            "domain_tag": obj["domain_tag"],
            "refs": 1,
            "holders": {agent_id},
        }
        self._release_ref(agent_id, ctx_id)
        self._refs_of.setdefault(agent_id, set()).add(new_id)
        if copied:
            self._copy_count += 1
            self._copy_bytes += len(content)  # 被复制对象（裁剪后）的字节长度
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

    # ---- 显式共享登记（S2-02 / S2-04 对接） -------------------------------------
    def grant(self, from_agent: str, ctx_id: str, to_agent: str) -> None:
        """把现有对象显式共享给另一 Agent：refs += 1、holders 登记（M5 语义）。

        隔离域边界：仅限同域显式登记；跨域 grant 被拒（未登记即隔离）。
        消息总线投递时经此方法完成「接收方 acquire」语义，配对 release。
        """
        obj = self._check_access(from_agent, ctx_id, "grant")
        if to_agent in obj["holders"]:
            # C1 防护：refs 恒等于 holders 大小，重复登记会破坏不变量
            log.warning("E_BAD_HANDLE grant %s -> %s: 已持有，拒绝重复登记", ctx_id, to_agent)
            raise RuntimeErr(E_BAD_HANDLE, f"{to_agent} 已持有 {ctx_id}，拒绝重复共享")
        if self._domain_of(to_agent) != obj["domain_tag"]:
            self._perm_denied += 1
            log.warning(
                "E_PERM grant %s %s -> %s: 跨域共享被拒（对象域=%s，越权拒绝 #%d）",
                ctx_id, from_agent, to_agent, obj["domain_tag"], self._perm_denied,
            )
            raise RuntimeErr(
                E_PERM,
                f"跨域共享被拒：{to_agent}（域 {self._domain_of(to_agent)}）"
                f"≠ 对象域 {obj['domain_tag']}",
            )
        obj["refs"] += 1
        obj["holders"].add(to_agent)
        self._refs_of.setdefault(to_agent, set()).add(ctx_id)
        log.info(
            "grant %s: %s -> %s (refs=%d)", ctx_id, from_agent, to_agent, obj["refs"]
        )

    # ---- 定长裁剪（S2-02 第 3 节） ----------------------------------------------
    def _trim(self, content: bytes, ctx_id: str) -> bytes:
        """超过阈值按定长裁剪（保留最近 max_context_bytes 字节），D3/D4。

        理由（S2-02）：可复现、可计数、无外部依赖；不做语义压缩，
        不引入推理框架（技术栈红线）。只改 bytes，不改 holders / refs（D4）。
        """
        if len(content) <= self._max_context_bytes:
            return content
        before = len(content)
        trimmed = content[-self._max_context_bytes:]
        self._trim_count += 1
        log.info(
            "trim %s: 定长裁剪 %d -> %d 字节（保留最近内容，裁剪 #%d）",
            ctx_id, before, len(trimmed), self._trim_count,
        )
        return trimmed

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
        """契约② 字段：copy_count / copy_bytes / share_hit_rate（+ 诊断量）。

        前三个字段名与口径随契约② 冻结，供 results.json 与 T3 采集对齐；
        perm_denied / trim_count 是 S2-02 的可数证据（越权拒绝、裁剪次数）。
        """
        share_hit_rate = (
            self._share_hits / self._acquire_calls if self._acquire_calls else 0.0
        )
        return {
            "copy_count": self._copy_count,
            "copy_bytes": self._copy_bytes,
            "share_hit_rate": round(share_hit_rate, 4),
            "perm_denied": self._perm_denied,
            "trim_count": self._trim_count,
        }

    # ---- 观测辅助（只读，供 Observer / 测试用） ----------------------------------
    def object_count(self) -> int:
        """存活上下文对象数（只读诊断）。"""
        return len(self._objects)
