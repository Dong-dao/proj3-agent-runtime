"""resource.py：统一句柄管理 + LLM stub / Tool / File / 内存配额（契约① 第 3 节）。

对应机制：P3 一切皆句柄（OS 出处：文件描述符与权限表）。

不变量（维护）：
- 句柄不可伪造：handle_id 由本表发放，查不到即 E_BAD_HANDLE；
- 越权必拒：非 owner 操作返回 E_PERM；缺权限位返回 E_PERM；
- 超额必拒：used + amount > quota 返回 E_QUOTA；
- 已关闭：对 CLOSED 句柄操作返回 E_CLOSED；
- release 归零才 CLOSED；release_all 全部回收（终态时由 AgentTable 调用）。

日志（产生）：
- acquire / use / release / release_all 各记一条 INFO 日志；
- 所有拒绝记 WARNING 日志并携带错误码。

计数：acquire / use / release / reject / reclaim 次数，按 kind 分组
（经 ResourceManager.stats() 导出，供 Observer / results.json 采集）。
"""

from __future__ import annotations

import logging
from dataclasses import replace

from .handle import (
    E_BAD_HANDLE,
    E_CLOSED,
    E_PERM,
    E_QUOTA,
    HandleKind,
    ResourceHandle,
    RuntimeErr,
    STATE_CLOSED,
    STATE_OPEN,
)

log = logging.getLogger("runtime.resource")

_VALID_PERMS = frozenset({"READ", "WRITE", "EXEC"})
# use() 所需权限位：模型与工具调用是执行（EXEC），文件读 READ、写 WRITE，
# 内存记账不需要权限位。
USE_PERM_BY_KIND = {
    HandleKind.LLM: "EXEC",
    HandleKind.TOOL: "EXEC",
    HandleKind.FILE: "READ",
    HandleKind.MEMORY: None,
}


class ResourceManager:
    """句柄表：发放、使用、释放、终态回收；单一写入者独占写权限。"""

    def __init__(self) -> None:
        self._handles: dict[str, ResourceHandle] = {}
        self._seq = 0
        self._counters: dict[str, int] = {
            "acquire": 0, "use": 0, "release": 0, "reject": 0, "reclaim": 0,
        }

    # ---- 发放 --------------------------------------------------------------
    def acquire(self, agent_id: str, kind: HandleKind, perms: frozenset[str], quota: int) -> ResourceHandle:
        """发放句柄（契约① 第 3 节签名）。

        维护：perms 只允许 READ/WRITE/EXEC 子集，quota 必须非负；
        产生：acquire INFO 日志；非法参数记 reject 并抛 E_PERM。
        """
        if not perms <= _VALID_PERMS or (not perms and kind is not HandleKind.MEMORY):
            self._counters["reject"] += 1
            log.warning("E_PERM acquire %s: perms=%s 非法", agent_id, sorted(perms))
            raise RuntimeErr(E_PERM, f"perms {sorted(perms)} 不是合法权限位集合")
        if quota < 0:
            self._counters["reject"] += 1
            log.warning("E_QUOTA acquire %s: quota=%d 非法", agent_id, quota)
            raise RuntimeErr(E_QUOTA, f"quota={quota} 不能为负")
        self._seq += 1
        handle = ResourceHandle(
            handle_id=f"H{self._seq:06d}",
            kind=kind,
            owner=agent_id,
            perms=frozenset(perms),
            quota=quota,
            used=0,
            refs=1,
            state=STATE_OPEN,
        )
        self._handles[handle.handle_id] = handle
        self._counters["acquire"] += 1
        log.info("acquire %s -> %s (%s, quota=%d)", agent_id, handle.handle_id, kind.name, quota)
        return handle

    def register(self, handle: ResourceHandle) -> ResourceHandle:
        """登记外部构造的句柄（供 agent.py 的终态回收对账使用）。"""
        self._handles[handle.handle_id] = handle
        return handle

    # ---- 使用 --------------------------------------------------------------
    def use(self, handle_id: str, agent_id: str, amount: int) -> None:
        """按句柄用量记账（契约① 第 3 节签名）。

        维护：used <= quota；
        产生：use INFO 日志；E_BAD_HANDLE / E_PERM / E_CLOSED / E_QUOTA 拒绝日志。
        """
        handle = self._handles.get(handle_id)
        if handle is None:
            self._counters["reject"] += 1
            log.warning("E_BAD_HANDLE use %s: 句柄不存在（伪造或过期）", handle_id)
            raise RuntimeErr(E_BAD_HANDLE, f"句柄 {handle_id} 不存在")
        if agent_id != handle.owner:
            self._counters["reject"] += 1
            log.warning("E_PERM use %s: %s 不是持有者 %s", handle_id, agent_id, handle.owner)
            raise RuntimeErr(E_PERM, f"{agent_id} 无权操作 {handle.owner} 的句柄 {handle_id}")
        if handle.state == STATE_CLOSED:
            self._counters["reject"] += 1
            log.warning("E_CLOSED use %s: 句柄已关闭", handle_id)
            raise RuntimeErr(E_CLOSED, f"句柄 {handle_id} 已关闭")
        need_perm = USE_PERM_BY_KIND[handle.kind]
        if need_perm is not None and need_perm not in handle.perms:
            self._counters["reject"] += 1
            log.warning("E_PERM use %s: 缺少 %s 权限位", handle_id, need_perm)
            raise RuntimeErr(E_PERM, f"句柄 {handle_id} 缺少 {need_perm} 权限位")
        if amount < 0 or handle.used + amount > handle.quota:
            self._counters["reject"] += 1
            log.warning(
                "E_QUOTA use %s: used=%d + amount=%d > quota=%d",
                handle_id, handle.used, amount, handle.quota,
            )
            raise RuntimeErr(
                E_QUOTA,
                f"句柄 {handle_id} 配额不足：used={handle.used} + {amount} > quota={handle.quota}",
            )
        self._handles[handle_id] = replace(handle, used=handle.used + amount)
        self._counters["use"] += 1
        log.info("use %s by %s: +%d (used=%d/%d)", handle_id, agent_id, amount,
                 handle.used + amount, handle.quota)

    # ---- 释放 --------------------------------------------------------------
    def release(self, handle_id: str, agent_id: str) -> None:
        """释放一次持有（refs -= 1，归零才 CLOSED）。"""
        handle = self._handles.get(handle_id)
        if handle is None:
            self._counters["reject"] += 1
            log.warning("E_BAD_HANDLE release %s: 句柄不存在", handle_id)
            raise RuntimeErr(E_BAD_HANDLE, f"句柄 {handle_id} 不存在")
        if agent_id != handle.owner:
            self._counters["reject"] += 1
            log.warning("E_PERM release %s: %s 不是持有者 %s", handle_id, agent_id, handle.owner)
            raise RuntimeErr(E_PERM, f"{agent_id} 无权释放 {handle.owner} 的句柄 {handle_id}")
        if handle.state == STATE_CLOSED:
            self._counters["reject"] += 1
            log.warning("E_CLOSED release %s: 句柄已关闭", handle_id)
            raise RuntimeErr(E_CLOSED, f"句柄 {handle_id} 已关闭")
        refs = handle.refs - 1
        self._handles[handle_id] = replace(
            handle, refs=refs, state=STATE_CLOSED if refs <= 0 else STATE_OPEN
        )
        self._counters["release"] += 1
        log.info("release %s by %s: refs=%d state=%s",
                 handle_id, agent_id, refs, self._handles[handle_id].state)

    def release_all(self, agent_id: str) -> int:
        """终态回收：关闭该 Agent 名下全部句柄，返回回收数量。

        维护：终态回收不变量（I4）；产生：reclaim INFO 日志。
        """
        reclaimed = 0
        for handle_id, handle in list(self._handles.items()):
            if handle.owner == agent_id and handle.state == STATE_OPEN:
                self._handles[handle_id] = replace(handle, state=STATE_CLOSED, refs=0)
                reclaimed += 1
        self._counters["reclaim"] += reclaimed
        if reclaimed:
            log.info("release_all %s: 回收 %d 个句柄", agent_id, reclaimed)
        return reclaimed

    # ---- 查询与计数 ----------------------------------------------------------
    def get(self, handle_id: str) -> ResourceHandle:
        """只读查询句柄（导出不改状态）。"""
        handle = self._handles.get(handle_id)
        if handle is None:
            raise RuntimeErr(E_BAD_HANDLE, f"句柄 {handle_id} 不存在")
        return handle

    def stats(self) -> dict:
        """计数导出：acquire / use / release / reject / reclaim 次数。"""
        return dict(self._counters)


# ---- stub 执行体（决定论：固定返回、固定逻辑延迟） -----------------------------


class LLMStub:
    """LLM stub：不接真实大模型，返回由 prompt 确定性推导的固定回复。

    决定论：同输入必同输出；延迟由调度器的模拟时钟按 slice_ms 计量，
    本类不做真实 sleep，保证实验可复现且跑得快。
    """

    def __init__(self, model_name: str = "stub-llm") -> None:
        self.model_name = model_name

    def run(self, prompt: str) -> str:
        """执行一次模型调用 stub：返回确定性回复。"""
        return f"{self.model_name}::reply(len={len(prompt)})"


class ToolStub:
    """工具 stub：按工具名返回确定性结果。"""

    def __init__(self, tool_name: str = "stub-tool") -> None:
        self.tool_name = tool_name

    def run(self, args: str) -> str:
        return f"{self.tool_name}::result(len={len(args)})"


class FileArea:
    """文件区 stub：受句柄与配额约束的键值存储。"""

    def __init__(self) -> None:
        self._files: dict[str, bytes] = {}

    def write(self, path: str, data: bytes) -> int:
        self._files[path] = data
        return len(data)

    def read(self, path: str) -> bytes:
        if path not in self._files:
            raise RuntimeErr(E_BAD_HANDLE, f"文件 {path} 不存在")
        return self._files[path]
