"""scheduler.py：依赖感知调度器 v1 + v2（契约① 第 5 节）。

对应机制：P1 机制下沉、策略上浮（OS 出处：CFS / EEVDF 的公平份额 + 时延约束
思想迁移；openEuler 24.03 LTS SP3 基于 Linux 6.6 内核已引入 EEVDF）。

v1（11/20）：依赖未满足不入队；同一任务不重复派发；所有状态变更经 transition()。
v2（11/21）：排序键 (eligible, vd, seq)，关键路径长的任务靠 weight 提前。

排序键（S1 冻结，契约① 第 5 节）：
    eligible = deps_ready and lag >= 0        lag = quota - used_slice（用户态配额账本）
    vd       = ready_time + slice_ms / weight  关键路径越长 weight 越大，vd 越早
    seq      = 递增提交序号                    同键按提交序，决定论兜底，不用随机数
policy="fifo" 为基线策略：只按 seq 排序，供 FIFO 基线对比（S1 首轮）。

不变量（维护）：
- 依赖未满足的任务不入就绪队列（_refresh_ready 只收 deps ⊆ done 的任务）；
- 同一任务不重复派发（next() 即摘除 + _dispatched 集合双保险）；
- 所有状态变更经 transition() 单一入口；next() 不改 Agent 状态；
- 单一写入者：Agent Table / TaskGraph / Handle Table / 计数器只由事件循环线程写，
  worker 线程只跑 stub 并通过 future 回传结果。

日志（产生）：
- submit / spawn / dispatch / complete / quota_reject 各记一条日志；
- 每轮调度决策记 DEBUG 日志（携带排序键），便于核对排序依据。

计数：就绪队列长度、等待时间、总完成时间、deadline 违约数（S1 占位口径，
S2 契约②冻结后以口径为准）、调度器自身耗时占比，经 stats() 导出。
"""

from __future__ import annotations

import heapq
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from .agent import AgentState, AgentTable, E_ILLEGAL_TRANSITION
from .graph import Task, TaskGraph
from .handle import HandleKind, RuntimeErr
from .resource import ResourceManager

log = logging.getLogger("runtime.scheduler")

POLICY_FIFO = "fifo"
POLICY_VD = "vd"
_VALID_POLICIES = (POLICY_FIFO, POLICY_VD)

# S1 占位口径：完成时刻超过 ready_time + slice_ms * 10 记一次 deadline 违约。
# 该系数是 S1 占位值，S2 契约②冻结口径后替换，不为数据好看改定义。
_DEADLINE_SLACK_FACTOR = 10.0


class Scheduler:
    """调度器：机制在内核（本类），策略由应用经 Task 的 deps/weight/quota 声明。"""

    def __init__(
        self,
        graph: TaskGraph | None = None,
        policy: str = POLICY_VD,
        workers: int = 2,
        table: AgentTable | None = None,
        resources: ResourceManager | None = None,
    ) -> None:
        if policy not in _VALID_POLICIES:
            raise ValueError(f"policy 只支持 {_VALID_POLICIES}，收到 {policy!r}")
        if workers < 1:
            raise ValueError("workers 必须 >= 1")
        self.graph = graph if graph is not None else TaskGraph()
        self.policy = policy
        self.workers = workers
        self.table = table if table is not None else AgentTable()
        self.resources = resources if resources is not None else ResourceManager()
        self.table.bind_resources(self.resources)

        self._seq = 0
        self._seq_of: dict[str, int] = {}       # task_id -> 提交序号（决定论兜底）
        self._pending: set[str] = set()         # CREATED：依赖未满足，未入队
        self._ready: dict[str, float] = {}      # task_id -> ready_time（就绪队列）
        self._dispatched: set[str] = set()      # 已派发集合：同一任务不重复派发
        self._done: set[str] = set()
        self._ready_time: dict[str, float] = {}
        self._start_time: dict[str, float] = {}
        self._finish_time: dict[str, float] = {}
        self._quota_used: dict[str, float] = {}  # task_id -> 已消耗 slice（lag 账本）
        self._sim_now = 0.0
        self._first_start: float | None = None
        self._last_finish: float | None = None
        self._sched_cpu = 0.0

    # ---- 登记接口（契约①） ---------------------------------------------------
    def submit(self, task: Task) -> None:
        """登记任务入图并转 CREATED；成环即拒绝（graph 内已回滚）。

        维护：图内无环、task_id 唯一；产生：submit INFO 日志。
        """
        self._timed(self._do_submit, task)

    def _do_submit(self, task: Task) -> None:
        self.graph.add_task(task)
        if task.agent_id not in self.table.all_agents():
            self.table.create(task.agent_id, quota=task.quota)
        self._seq += 1
        self._seq_of[task.task_id] = self._seq
        self._quota_used[task.task_id] = 0.0
        self._pending.add(task.task_id)
        log.info("submit %s (seq=%d, deps=%s, policy=%s)",
                 task.task_id, self._seq, list(task.deps), self.policy)

    def spawn(self, parent_id: str, task: Task) -> None:
        """动态子任务注入：graph.spawn 继承 quota/weight 并重测环，随后转 CREATED。

        维护：子任务继承父任务配额账本与权重基线；成环拒绝（死锁防护）；
        产生：spawn INFO 日志（含继承值）、E_DAG_CYCLE 拒绝日志（成环时）。
        """
        self._timed(self._do_spawn, parent_id, task)

    def _do_spawn(self, parent_id: str, task: Task) -> None:
        self.graph.spawn(parent_id, task)
        if task.agent_id not in self.table.all_agents():
            self.table.create(task.agent_id, quota=task.quota)
        self._seq += 1
        self._seq_of[task.task_id] = self._seq
        self._quota_used[task.task_id] = 0.0
        self._pending.add(task.task_id)
        log.info("spawn %s -> %s (seq=%d, deps=%s)",
                 parent_id, task.task_id, self._seq, list(task.deps))

    # ---- 派发接口（契约①） ---------------------------------------------------
    def next(self) -> Task | None:
        """取排序键最优的就绪任务；无 eligible 返回 None。

        不变量：依赖未满足不入队（先 _refresh_ready）；同一任务不重复派发
        （选中即摘出就绪队列）；不改 Agent 状态（转换由事件循环统一执行）。
        """
        return self._timed(self._do_next)

    def _do_next(self) -> Task | None:
        self._refresh_ready()
        candidates = [tid for tid in self._ready if tid not in self._dispatched]
        if not candidates:
            return None
        best_tid = min(candidates, key=self._sort_key)
        self._ready.pop(best_tid)
        task = self.graph.nodes[best_tid]
        log.debug("pick %s (key=%s, policy=%s)", best_tid, self._sort_key(best_tid), self.policy)
        return task

    def _sort_key(self, task_id: str) -> tuple:
        """S1 冻结排序键：(eligible, vd, seq)。

        - vd = ready_time + slice_ms / weight：时延敏感（slice 小、weight 大）者
          vd 近、先跑；长任务靠 weight 抬高 slice/weight 份额，避免被短任务饿死；
        - seq 兜底：同键按提交序，不用随机数（决定论）。
        policy="fifo" 退化为 (seq,)，即 FIFO 基线。
        """
        seq = self._seq_of[task_id]
        if self.policy == POLICY_FIFO:
            return (seq,)
        task = self.graph.nodes[task_id]
        vd = self._ready_time[task_id] + task.slice_ms / max(task.weight, 1e-9)
        return (vd, seq)

    def dispatch(self, task: Task) -> str | None:
        """事件循环派发：READY -> RUNNING，申请 LLM 句柄；配额透支则配额拒绝。

        维护：所有状态变更经 transition()；lag 账本（quota_used += slice_ms）；
        产生：dispatch INFO 日志；配额拒绝 WARNING 日志 + READY -> FAILED 转换。
        返回句柄 id；配额拒绝返回 None。
        """
        return self._timed(self._do_dispatch, task)

    def _do_dispatch(self, task: Task) -> str | None:
        agent = self.table.get(task.agent_id)
        if agent.state == AgentState.CREATED:
            self.table.transition(task.agent_id, AgentState.READY)
            agent = self.table.get(task.agent_id)
        if agent.state != AgentState.READY:
            raise RuntimeErr(
                E_ILLEGAL_TRANSITION,
                f"agent {task.agent_id} 处于 {agent.state.name}，无法派发 {task.task_id}"
                "（S1 口径：一个 Agent 同时只有一个活跃任务）",
            )
        # lag 账本：配额透支的 Ready 任务不得派发（eligible 的 lag >= 0 条款）
        used = self._quota_used[task.task_id] + task.slice_ms
        if used > task.quota:
            self._quota_used[task.task_id] = used
            log.warning(
                "E_QUOTA quota_reject %s: used=%.1f > quota=%d，READY -> FAILED",
                task.task_id, used, task.quota,
            )
            self.table.transition(task.agent_id, AgentState.FAILED)
            self._dispatched.add(task.task_id)
            return None
        self.table.transition(task.agent_id, AgentState.RUNNING)
        self._quota_used[task.task_id] = used
        self._dispatched.add(task.task_id)
        self._start_time[task.task_id] = self._sim_now
        if self._first_start is None:
            self._first_start = self._sim_now
        handle = self.resources.acquire(
            task.agent_id, HandleKind.LLM, frozenset({"EXEC"}), quota=task.quota
        )
        self.table.get(task.agent_id).handles.add(handle.handle_id)
        return handle.handle_id

    def on_complete(self, task_id: str, result: object) -> None:
        """任务完成：RUNNING -> TERMINATED，终态回收句柄，下游唤醒。

        维护：终态回收（I4，经 transition() 内部触发）；done 集合推进；
        产生：complete INFO 日志（含等待时间与完成时刻）。
        """
        self._timed(self._do_on_complete, task_id, result)

    def _do_on_complete(self, task_id: str, result: object) -> None:
        task = self.graph.nodes[task_id]
        self._finish_time[task_id] = self._sim_now
        self._last_finish = self._sim_now
        self.table.transition(task.agent_id, AgentState.TERMINATED)
        self._done.add(task_id)
        wait = self._start_time.get(task_id, 0.0) - self._ready_time.get(task_id, 0.0)
        log.info("complete %s (agent=%s, wait=%.1fms, finish=%.1fms)",
                 task_id, task.agent_id, max(wait, 0.0), self._sim_now)

    def on_fail(self, task_id: str, reason: str) -> None:
        """任务失败：RUNNING -> FAILED（终态），故障域处理在 S2 接管。"""
        self._timed(self._do_on_fail, task_id, reason)

    def _do_on_fail(self, task_id: str, reason: str) -> None:
        task = self.graph.nodes[task_id]
        self._finish_time[task_id] = self._sim_now
        self._last_finish = self._sim_now
        log.warning("fail %s (agent=%s): %s", task_id, task.agent_id, reason)
        self.table.transition(task.agent_id, AgentState.FAILED)
        self._done.add(task_id)

    # ---- 事件循环（单一写入者） -----------------------------------------------
    def run(self, stub, executor: ThreadPoolExecutor | None = None) -> dict:
        """单线程事件循环：派发 -> worker 跑 stub -> 按模拟时钟消费完成事件。

        决定论：完成顺序按 (finish_time, 提交序) 处理，与线程实际返回顺序无关；
        stub 是纯函数（同输入同输出），同一负载重复运行结果一致。
        单一写入者：状态表只在本循环线程内写；worker 只执行 stub.run。
        """
        own_executor = executor is None
        if own_executor:
            executor = ThreadPoolExecutor(max_workers=self.workers)
        inflight: list[tuple[float, int, str, object]] = []  # (finish, order, task_id, future)
        order = 0
        wall_start = time.perf_counter()
        try:
            while True:
                while len(inflight) < self.workers:
                    task = self.next()
                    if task is None:
                        break
                    handle_id = self.dispatch(task)
                    if handle_id is not None:
                        future = executor.submit(stub.run, f"{task.task_id}:{task.slice_ms}")
                        finish = self._sim_now + task.slice_ms
                        heapq.heappush(inflight, (finish, order, task.task_id, future))
                        order += 1
                    else:
                        # 配额拒绝：READY -> FAILED 已发生，本轮继续取下一个
                        continue
                if not inflight:
                    if self._pending:
                        raise RuntimeError(
                            "就绪队列为空但仍有未满足依赖的任务，疑似死锁: "
                            f"{sorted(self._pending)}"
                        )
                    break
                # 推进模拟时钟到最早完成事件，消费结果（单写入者）
                finish, _, task_id, future = heapq.heappop(inflight)
                self._sim_now = finish
                result = future.result()
                self.on_complete(task_id, result)
        finally:
            if own_executor:
                executor.shutdown(wait=True)
        wall = time.perf_counter() - wall_start
        self._stats_wall = wall
        return self.stats()

    # ---- 内部 ---------------------------------------------------------------
    def _refresh_ready(self) -> None:
        """依赖全部完成的 CREATED 任务入就绪队列（READY），记录就绪时刻。

        不变量：依赖未满足的任务不入队。
        """
        newly_ready = [
            tid for tid in self._pending
            if set(self.graph.nodes[tid].deps) <= self._done
        ]
        for tid in sorted(newly_ready, key=lambda t: self._seq_of[t]):  # 决定论
            self._pending.discard(tid)
            self._ready_time[tid] = self._sim_now
            self._ready[tid] = self._sim_now
            agent = self.table.get(self.graph.nodes[tid].agent_id)
            if agent.state == AgentState.CREATED:
                self.table.transition(agent.agent_id, AgentState.READY)
            elif agent.state == AgentState.TERMINATED:
                # 同一 Agent 的上一个任务已完成：S1 简化口径下不再复用状态
                log.debug("agent %s 已终态，任务 %s 仍按就绪记账", agent.agent_id, tid)

    def _timed(self, fn, *args):
        """调度器自身耗时计量（sched_overhead_pct 的采集点）。"""
        t0 = time.perf_counter()
        try:
            return fn(*args)
        finally:
            self._sched_cpu += time.perf_counter() - t0

    # ---- 计数导出 -------------------------------------------------------------
    def stats(self) -> dict:
        """七项指标相关采集点（契约② 草稿口径，S2 冻结后为准）。

        返回：ready_len / makespan_ms / waiting_ms_total / deadline_miss /
        sched_overhead_pct / 状态机计数（transitions / illegal_intercepts /
        reclaimed_handles）。
        """
        makespan = 0.0
        if self._first_start is not None and self._last_finish is not None:
            makespan = self._last_finish - self._first_start
        waiting_total = sum(
            max(self._start_time.get(tid, 0.0) - rt, 0.0)
            for tid, rt in self._ready_time.items()
        )
        deadline_miss = 0
        for tid, rt in self._ready_time.items():
            if tid in self._finish_time:
                task = self.graph.nodes[tid]
                if self._finish_time[tid] > rt + task.slice_ms * _DEADLINE_SLACK_FACTOR:
                    deadline_miss += 1
        agent_stats = self.table.stats()
        overhead = 0.0
        wall = getattr(self, "_stats_wall", 0.0)
        if wall > 0:
            overhead = self._sched_cpu / wall * 100.0
        return {
            "ready_len": len(self._ready),
            "makespan_ms": makespan,
            "waiting_ms_total": waiting_total,
            "deadline_miss": deadline_miss,
            "sched_overhead_pct": overhead,
            "transitions": agent_stats["transitions"],
            "illegal_intercepts": agent_stats["illegal_intercepts"],
            "reclaimed_handles": agent_stats["reclaimed_handles"],
        }
