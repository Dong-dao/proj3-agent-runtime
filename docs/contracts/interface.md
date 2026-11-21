# 契约①：接口契约 v1（T1 → T2）

> 提供方：董博文（T1 机制设计）　接收方：陈苗苗（T2 Runtime 实现）　冻结日期：2026-11-17
> 落盘路径：`docs/contracts/interface.md`　状态：**v1 冻结**
>
> 本契约是 T1 与 T2 的唯一接口基线。约定：**字段可加，语义不可改**。任何签名调整先在群里说明影响哪几个模块，由董博文记入本文件末尾的「变更记录」，再同步给陈苗苗；未经记录的口头改动一律不生效。
> 所有公开方法必须在 docstring 里写清两件事：**维护哪条不变量、产生哪条日志**。这条是工程规范 20 分的自查项，不是可选项。

---

## 0. 契约适用范围与前置约定

本契约冻结五组接口，对应本周规划第二节步骤 1 的五项内容：

1. Agent 状态枚举 `AgentState`
2. TaskGraph 结构 `Task` / `TaskGraph`
3. ResourceHandle 字段 `ResourceHandle` / `HandleKind`
4. ContextStore 方法签名 `ContextStore`
5. 调度器事件循环接口 `Scheduler` + 全局 `transition()`

三条前置约定贯穿全部签名，实现时不得违背：

- **单一状态入口**：任何 Agent 状态变更只能经 `transition()`，禁止在模块内直接给 `state` 赋值。
- **单一写入者**：调度器事件循环独占 Agent Table / TaskGraph / Handle Table / 计数器的写权限；worker 线程只跑 stub，完成后把结果放回队列，不直接改状态表。
- **决定论**：排序键末位用递增 `seq`，同键按提交序，不用随机数、不用系统时间做排序依据。

---

## 1. Agent 状态枚举（六态）

```python
from enum import Enum

class AgentState(Enum):
    CREATED = 1       # 已登记，依赖未满足，尚未入队
    READY = 2         # 依赖满足，在就绪队列中等待派发
    RUNNING = 3       # 已被派发，占用 worker 执行 stub
    BLOCKED = 4       # 等待 IO / 句柄 / 子任务，可回到 READY
    TERMINATED = 5    # 正常完成，终态
    FAILED = 6        # 超时 / 异常 / 配额拒绝 / 熔断，终态
```

**不变量**：任一时刻一个 Agent 只能处于唯一状态；`TERMINATED` 与 `FAILED` 为终态，不可回退；进入终态时 `release_all()` 回收全部句柄与上下文引用；非法转换返回 `E_ILLEGAL_TRANSITION` 并记一条拒绝日志。

**合法转换矩阵（6×6，实现须逐格覆盖单测）** 见 `docs/design/六态状态机与合法转换矩阵.md`，本契约引用该矩阵，语义以矩阵为准。

---

## 2. Task 与 TaskGraph 结构

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class Task:
    task_id: str              # 全局唯一
    agent_id: str             # 所属 Agent
    deps: tuple[str, ...]     # 前置任务 task_id 集合；空表示无依赖
    weight: float             # 权重，参与 vd 计算（关键路径越长越大）
    slice_ms: float           # 时间片预算，stub 下由 prompt 长度 / 工具数固定
    quota: int                # 逻辑配额账本初值

@dataclass
class TaskGraph:
    nodes: dict[str, Task]
    edges: dict[str, frozenset[str]]   # task_id -> 它依赖的前置 task_id 集合
```

`TaskGraph` 方法签名（.graph.py）：

```python
class TaskGraph:
    def add_task(self, task: Task) -> None: ...
    def add_edge(self, predecessor: str, successor: str) -> None: ...
    def detect_cycle(self) -> list[str] | None: ...          # 返回环路径；无环返回 None
    def ready_set(self, done: frozenset[str]) -> tuple[str, ...]: ...  # deps 全在 done 中的任务
    def critical_path_remaining(self, task_id: str) -> float: ...      # 供 weight 使用
    def spawn(self, parent_id: str, child: Task) -> None: ...           # 动态子任务注入
```

- `add_edge(predecessor, successor)` 语义：`successor` 依赖 `predecessor`（predecessor 是前置）。
- `detect_cycle()` 新增任务或 `spawn()` 之后必须调用；有环则返回环路径并拒绝入图，记 `E_DAG_CYCLE` 日志（`spawn` 死锁防护的一环）。
- `spawn()` 子集继承父任务的 `quota` 账本与 `weight` 基线，继承后重新做环检测——避免动态子任务与父任务形成环导致死锁。

---

## 3. ResourceHandle 字段与错误码

```python
from enum import Enum
from dataclasses import dataclass

class HandleKind(Enum):
    LLM = 1
    TOOL = 2
    FILE = 3
    MEMORY = 4

@dataclass(frozen=True)
class ResourceHandle:
    handle_id: str            # Runtime 统一发放，不可伪造
    kind: HandleKind
    owner: str                # agent_id，随实体终止而回收
    perms: frozenset[str]     # READ / WRITE / EXEC
    quota: int
    used: int
    refs: int                 # 引用计数
    state: str                # OPEN / CLOSED
```

句柄操作接口（.handle.py / resource.py）：

```python
def acquire(agent_id: str, kind: HandleKind, perms: frozenset[str], quota: int) -> ResourceHandle: ...
def use(handle_id: str, agent_id: str, amount: int) -> None: ...
def release(handle_id: str, agent_id: str) -> None: ...
def release_all(agent_id: str) -> int: ...   # 返回回收数量，进入终态时调用
```

**不变量**：句柄不可伪造；越权返回 `E_PERM`，超额返回 `E_QUOTA`，伪造或过期返回 `E_BAD_HANDLE`，已关闭返回 `E_CLOSED`；实体进入终态时 `release_all()` 全部回收并记账。

**错误码集合（固定，不得新增语义）**：
`E_ILLEGAL_TRANSITION` / `E_PERM` / `E_QUOTA` / `E_BAD_HANDLE` / `E_CLOSED`。
DAG 环检测另用 `E_DAG_CYCLE`（日志与计数用，不占用上述公开错误码）。

---

## 4. ContextStore 方法签名

> S1 阶段：本契约先冻结方法签名；COW 的写时复制语义在 S2 实现，S1 允许 stub 实现但不改签名（本周规划第三节红线：S1 不做 COW 实体）。

```python
class ContextStore:
    def acquire(self, agent_id: str, prefix_id: str) -> str: ...
        # 维护：refs 恒等于持有者数量；产生：acquire 计数日志
    def read(self, agent_id: str, ctx_id: str) -> bytes: ...
        # 维护：跨域读取校验，越权抛 E_PERM；产生：域校验日志
    def write(self, agent_id: str, ctx_id: str, data: bytes) -> str: ...
        # 维护：仅当 refs > 1 才复制；产生：copy_count / copy_bytes 计数日志
    def release(self, agent_id: str, ctx_id: str) -> None: ...
        # 维护：refs 归零才释放；产生：release 计数日志
    def stats(self) -> dict: ...
        # 返回：copy_count / copy_bytes / share_hit_rate 当前值
```

**不变量**：`refs` 恒等于持有者数量；只有写入且 `refs > 1` 才发生真实复制；归零才释放；跨隔离域读取被拒绝。

---

## 5. 调度器事件循环接口

```python
class Scheduler:
    def submit(self, task: Task) -> None: ...            # 登记入图，转 CREATED，检测环
    def spawn(self, parent_id: str, task: Task) -> None: ...  # 动态子任务，继承 + 重测环
    def next(self) -> Task | None: ...                   # 取排序键最优者，无 eligible 返回 None
    def on_complete(self, task_id: str, result: object) -> None: ...  # 回收 + 下游唤醒

def transition(agent_id: str, to: AgentState) -> None: ...
    # 唯一状态入口：校验合法转换矩阵，非法则抛 E_ILLEGAL_TRANSITION 并记日志
```

**排序键（S1 冻结为 v1，写进 scheduler.py 注释）**：
`(eligible, vd, seq)`，其中 `eligible = deps_ready and lag >= 0`，`vd = eligible_time + slice_ms / weight`，`seq` 为递增提交序号。每轮只从 eligible 集合取 `vd` 最小者派发。

**不变量**：依赖未满足的任务不入就绪队列；同一任务不重复派发；所有状态变更经 `transition()`；`next()` 不改 Agent 状态（转换由事件循环消费结果后统一执行）。

**计数接口**：`scheduler.stats()` 返回队列长度、等待时间、总完成时间、deadline 违约数、调度器自身耗时（供七项指标中的「调度开销」采集）。

---

## 6. 双方确认记录

按本周规划第二节步骤 2，双方各留一句确认；陈苗苗逐条对照签名，有歧义当场改，不憋到周末。

- 董博文（T1）：__________　日期：2026-11-17
- 陈苗苗（T2）：__________　日期：__________

---

## 7. 变更记录（字段可加，语义不可改）

| 版本 | 日期 | 变更内容 | 影响模块 | 提出人 | 记录人 |
| --- | --- | --- | --- | --- | --- |
| v1 | 2026-11-17 | 首次冻结，五组接口全部定稿 | agent / graph / scheduler / handle / context | 董博文 | 董博文 |

> 后续每次变更在本表追加一行，写明影响哪几个模块；语义变更一律不接受，只能新增字段或新增方法。

依据：《proj3 本周规划｜S1 11月17-23》第二节、《2026OS区域赛proj3学习路线图》6.1 节、《2026 OS 区域赛 proj3 设计理念、技术路线与技术栈》第六章。契约①冻结后字段可加不可改语义。
