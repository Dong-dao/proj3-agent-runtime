"""graph.py：依赖 DAG、环检测、动态子任务（契约① 第 2 节）。

对应机制：依赖感知调度的图基础（OS 出处：进程依赖与拓扑序）。

不变量（维护）：
- 图内无环：任何可能成环的写操作（add_task / add_edge / spawn）先试加、
  后检测，成环即回滚并抛 E_DAG_CYCLE —— 这是动态子任务死锁防护的一环；
- task_id 全局唯一，重复登记被拒绝；
- deps 中引用的前置任务必须已在图中。

日志（产生）：
- 成环拒绝记 WARNING 日志（携带 E_DAG_CYCLE）；
- spawn 成功记 INFO 日志，携带继承来的 quota / weight。

计数：add_task / add_edge / spawn / cycle_reject 次数（经 stats() 导出）。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .handle import E_DAG_CYCLE, RuntimeErr

log = logging.getLogger("runtime.graph")


@dataclass(frozen=True)
class Task:
    """任务实体（契约① 第 2 节，frozen：提交后不可变，改用新 Task 重新提交）。"""

    task_id: str              # 全局唯一
    agent_id: str             # 所属 Agent
    deps: tuple[str, ...]     # 前置任务 task_id 集合；空表示无依赖
    weight: float             # 权重，参与 vd 计算（关键路径越长越大）
    slice_ms: float           # 时间片预算，stub 下由 prompt 长度 / 工具数固定
    quota: int                # 逻辑配额账本初值


@dataclass
class TaskGraph:
    """依赖 DAG：nodes 按插入序（决定论的 seq 兜底用它）；edges[task_id] = 前置集合。"""

    nodes: dict[str, Task]
    edges: dict[str, frozenset[str]]   # task_id -> 它依赖的前置 task_id 集合

    def __init__(self) -> None:
        self.nodes = {}
        self.edges = {}
        self._dependents: dict[str, set[str]] = {}  # 反向邻接：task_id -> 依赖它的任务
        self._cpr_cache: dict[str, float] | None = None
        self._counters = {"add_task": 0, "add_edge": 0, "spawn": 0, "cycle_reject": 0}

    # ---- 登记 --------------------------------------------------------------
    def add_task(self, task: Task) -> None:
        """登记任务节点并按 task.deps 建边；成环则整体回滚。

        维护：task_id 唯一、deps 前置必须已在图中、图内无环；
        产生：E_DAG_CYCLE 拒绝日志（成环时）。
        """
        if task.task_id in self.nodes:
            log.warning("duplicate task_id rejected: %s", task.task_id)
            raise ValueError(f"task_id {task.task_id} 已存在，拒绝重复登记")
        missing = [dep for dep in task.deps if dep not in self.nodes]
        if missing:
            raise ValueError(f"task {task.task_id} 的前置任务不在图中: {missing}")
        # 试加：先入图再检测，成环整体回滚
        self._insert(task)
        cycle = self.detect_cycle()
        if cycle is not None:
            self._remove(task.task_id)
            self._counters["cycle_reject"] += 1
            log.warning("E_DAG_CYCLE add_task %s: 成环 %s，已回滚", task.task_id, " -> ".join(cycle))
            raise RuntimeErr(E_DAG_CYCLE, f"新增任务 {task.task_id} 使图成环: {' -> '.join(cycle)}")
        self._counters["add_task"] += 1
        log.info("add_task %s (agent=%s, deps=%s)", task.task_id, task.agent_id, list(task.deps))

    def add_edge(self, predecessor: str, successor: str) -> None:
        """加一条依赖边：successor 依赖 predecessor（predecessor 是前置）。

        成环则拒绝并抛 E_DAG_CYCLE（边不会入图）。
        """
        if predecessor not in self.nodes or successor not in self.nodes:
            raise ValueError(f"边的端点不在图中: {predecessor}, {successor}")
        if predecessor == successor:
            self._counters["cycle_reject"] += 1
            log.warning("E_DAG_CYCLE add_edge: 自环 %s", predecessor)
            raise RuntimeErr(E_DAG_CYCLE, f"自环 {predecessor} 不允许")
        self.edges[successor] = self.edges[successor] | {predecessor}
        self._dependents[predecessor].add(successor)
        cycle = self.detect_cycle()
        if cycle is not None:
            self.edges[successor] = self.edges[successor] - {predecessor}
            self._dependents[predecessor].discard(successor)
            self._counters["cycle_reject"] += 1
            log.warning(
                "E_DAG_CYCLE add_edge %s -> %s: 成环 %s，已拒绝",
                predecessor, successor, " -> ".join(cycle),
            )
            raise RuntimeErr(E_DAG_CYCLE, f"边 {predecessor}->{successor} 使图成环")
        self._counters["add_edge"] += 1
        log.info("add_edge %s -> %s", predecessor, successor)

    def spawn(self, parent_id: str, child: Task) -> None:
        """动态子任务注入（契约①）：子任务继承父任务的 quota 账本与 weight 基线。

        继承规则：child.quota <= 0 时继承父 quota；child.weight <= 0 时继承父
        weight。继承后重新做环检测，成环则拒绝注入（避免动态子任务与父任务
        形成环导致死锁）。
        """
        if parent_id not in self.nodes:
            raise ValueError(f"父任务 {parent_id} 不在图中")
        if child.task_id in self.nodes:
            raise ValueError(f"task_id {child.task_id} 已存在，拒绝重复登记")
        parent = self.nodes[parent_id]
        inherited_quota = parent.quota if child.quota <= 0 else child.quota
        inherited_weight = parent.weight if child.weight <= 0 else child.weight
        child = Task(
            task_id=child.task_id,
            agent_id=child.agent_id,
            deps=child.deps,
            weight=inherited_weight,
            slice_ms=child.slice_ms,
            quota=inherited_quota,
        )
        missing = [dep for dep in child.deps if dep not in self.nodes]
        if missing:
            raise ValueError(f"子任务 {child.task_id} 的前置任务不在图中: {missing}")
        self._insert(child)
        cycle = self.detect_cycle()
        if cycle is not None:
            self._remove(child.task_id)
            self._counters["cycle_reject"] += 1
            log.warning(
                "E_DAG_CYCLE spawn %s -> %s: 成环 %s，拒绝注入",
                parent_id, child.task_id, " -> ".join(cycle),
            )
            raise RuntimeErr(
                E_DAG_CYCLE,
                f"子任务 {child.task_id} 使图成环，拒绝注入: {' -> '.join(cycle)}",
            )
        self._counters["spawn"] += 1
        log.info(
            "spawn %s -> %s (继承 quota=%d, weight=%.1f)",
            parent_id, child.task_id, inherited_quota, inherited_weight,
        )

    # ---- 查询 --------------------------------------------------------------
    def detect_cycle(self) -> list[str] | None:
        """返回一条环路径（如 [A, B, C, A]）；无环返回 None。DFS 染色实现。"""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {tid: WHITE for tid in self.nodes}
        stack: list[str] = []

        def dfs(u: str) -> list[str] | None:
            color[u] = GRAY
            stack.append(u)
            for v in sorted(self._dependents.get(u, ())):  # 排序保证决定论
                if color[v] == GRAY:
                    idx = stack.index(v)
                    return stack[idx:] + [v]  # 环：v...u 回到 v
                if color[v] == WHITE:
                    found = dfs(v)
                    if found is not None:
                        return found
            color[u] = BLACK
            stack.pop()
            return None

        for tid in self.nodes:  # dict 按插入序，天然决定论
            if color[tid] == WHITE:
                found = dfs(tid)
                if found is not None:
                    return found
        return None

    def ready_set(self, done: frozenset[str]) -> tuple[str, ...]:
        """deps 全部在 done 中的任务 id 元组（按插入序，供调度器筛选入队）。"""
        return tuple(
            tid for tid, task in self.nodes.items()
            if set(task.deps) <= set(done)
        )

    def critical_path_remaining(self, task_id: str) -> float:
        """从 task_id 到汇点的最长 slice_ms 链长（含自身），供 weight 使用。

        记忆化缓存，任何图变更后整体失效重算。
        """
        if self._cpr_cache is None:
            self._cpr_cache = {}
            # 逆拓扑序：先算汇点，再回推
            order = self._topological_order()
            for tid in reversed(order):
                downstream = self._dependents.get(tid, ())
                best = max(
                    (self._cpr_cache.get(dep_tid, 0.0) for dep_tid in downstream),
                    default=0.0,
                )
                self._cpr_cache[tid] = self.nodes[tid].slice_ms + best
        if task_id not in self._cpr_cache:
            raise KeyError(f"task {task_id} 不在图中")
        return self._cpr_cache[task_id]

    def dependents(self, task_id: str) -> frozenset[str]:
        """依赖 task_id 的任务集合（下游唤醒用）。"""
        return frozenset(self._dependents.get(task_id, ()))

    # ---- 内部 --------------------------------------------------------------
    def _insert(self, task: Task) -> None:
        self.nodes[task.task_id] = task
        self.edges[task.task_id] = frozenset(task.deps)
        self._dependents.setdefault(task.task_id, set())
        for dep in task.deps:
            self._dependents.setdefault(dep, set()).add(task.task_id)
        self._cpr_cache = None

    def _remove(self, task_id: str) -> None:
        task = self.nodes.pop(task_id, None)
        if task is None:
            return
        for dep in self.edges.pop(task_id, frozenset()):
            self._dependents.get(dep, set()).discard(task_id)
        for dependent in self._dependents.pop(task_id, set()):
            self.edges[dependent] = self.edges[dependent] - {task_id}
        self._cpr_cache = None

    def _topological_order(self) -> list[str]:
        indegree = {tid: len(self.edges[tid]) for tid in self.nodes}
        queue = sorted(tid for tid, deg in indegree.items() if deg == 0)
        order: list[str] = []
        while queue:
            tid = queue.pop(0)
            order.append(tid)
            for dependent in sorted(self._dependents.get(tid, ())):
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    queue.append(dependent)
        if len(order) != len(self.nodes):  # 理论不可达：入图前已保证无环
            raise RuntimeErr(E_DAG_CYCLE, "图内出现环，拓扑排序失败")
        return order

    def stats(self) -> dict:
        """计数导出：add_task / add_edge / spawn / cycle_reject 次数。"""
        return dict(self._counters)
