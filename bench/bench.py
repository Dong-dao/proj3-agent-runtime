"""bench.py：S1 FIFO 基线与首轮对比（T2 陈苗苗接入真实 runtime）。

命令（本周规划第四节步骤 5）：
    python bench.py --baseline serial,fifo --repeat 3 --out results.json
    python -m json.tool results.json > /dev/null   # 校验 JSON 合法性

三基线口径（S1 首轮；S3 全量实验沿用同一入口）：
- serial  ：workers=1 + FIFO 序 —— 无并发，排除「并行本身」的贡献；
- fifo    ：workers=N + FIFO 序 —— 有并发，按提交序派发（FIFO 基线）；
- naive   ：S1 与 fifo 等价（COW 上下文 S2 才实现），detail.policy 注明；
- runtime ：workers=N + v2 排序键 (eligible, vd, seq) —— 我们的机制。

决定论：负载固定（scenario.TASKS）、LLM stub 纯函数、模拟时钟事件驱动、
同键按提交序。同一负载重复运行结果一致；--repeat 3 保留每次原始值，
禁止改动原始 JSON。

指标口径：makespan_ms / waiting_ms_total 来自调度器模拟时钟；sched_overhead_pct
为调度器真实 CPU 耗时占真实墙钟的比例（S1 占位口径，S2 契约②冻结后以口径
为准）；peak_rss_mb 用 tracemalloc 峰值（Windows 无 resource.getrusage，
跨平台等价采集点，S2 口径冻结时统一）。
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import tracemalloc
from datetime import datetime
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
_ROOT = _BENCH_DIR.parent
for _p in (str(_BENCH_DIR), str(_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from runtime.graph import Task, TaskGraph  # noqa: E402
from runtime.resource import LLMStub  # noqa: E402
from runtime.scheduler import POLICY_FIFO, POLICY_VD, Scheduler  # noqa: E402
from scenario import TASKS  # noqa: E402

SEED = 20261111
VALID_BASELINES = ("serial", "fifo", "naive", "runtime")


def build_tasks() -> tuple[list[Task], float]:
    """把 T3 的负载定义（scenario.TASKS）转成契约① 的 Task 列表。

    应用声明（P1 策略上浮）：weight 按关键路径剩余长度声明（关键路径越长
    weight 越大），slice_ms 取负载定义的 duration_ms，quota 给足总预算
    （S1 首轮不做配额收紧实验）。agent 一任务一个（S1 口径：一个 Agent
    同时只有一个活跃任务）。
    """
    graph = TaskGraph()
    for spec in TASKS:
        graph.add_task(
            Task(spec["task_id"], f"agent-{spec['task_id']}",
                 tuple(spec["depends_on"]), 1.0, float(spec["duration_ms"]), 0)
        )
    total = sum(float(spec["duration_ms"]) for spec in TASKS)
    tasks = [
        Task(
            task_id=spec["task_id"],
            agent_id=f"agent-{spec['task_id']}",
            deps=tuple(spec["depends_on"]),
            weight=graph.critical_path_remaining(spec["task_id"]),
            slice_ms=float(spec["duration_ms"]),
            quota=int(total) + 1,
        )
        for spec in TASKS
    ]
    return tasks, total


def run_once(baseline: str) -> dict:
    """跑一轮指定基线，返回契约② 结构的 metrics + detail。"""
    tasks, _total = build_tasks()
    if baseline == "serial":
        sched = Scheduler(policy=POLICY_FIFO, workers=1)
    elif baseline in ("fifo", "naive"):
        sched = Scheduler(policy=POLICY_FIFO, workers=3)
    else:  # runtime
        sched = Scheduler(policy=POLICY_VD, workers=3)
    for task in tasks:
        sched.submit(task)
    tracemalloc.start()
    stats = sched.run(LLMStub())
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    context_stats = {"copy_count": 0, "copy_bytes": 0, "share_hit_rate": 0.0}
    if baseline == "naive":
        note = "S1 与 fifo 等价（COW 上下文 S2 实现），不作独立结论"
    else:
        note = ""
    return {
        "baseline": baseline,
        "load": "s1_abc_stub",
        "repeat": 0,  # 由调用方回填
        "metrics": {
            "makespan_ms": round(stats["makespan_ms"], 3),
            "copy_count": context_stats["copy_count"],
            "copy_bytes": context_stats["copy_bytes"],
            "peak_rss_mb": round(peak / (1024 * 1024), 3),
            "sched_overhead_pct": round(stats["sched_overhead_pct"], 3),
            "recovery_ms": None,
            "affected_tasks": None,
        },
        "detail": {
            "policy": "vd" if baseline == "runtime" else "fifo",
            "workers": 1 if baseline == "serial" else 3,
            "waiting_ms_total": round(stats["waiting_ms_total"], 3),
            "ready_len": stats["ready_len"],
            "deadline_miss": stats["deadline_miss"],
            "transitions": stats["transitions"],
            "illegal_intercepts": stats["illegal_intercepts"],
            "reclaimed_handles": stats["reclaimed_handles"],
            "note": note,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="S1 三基线一键实验（接入真实 runtime）")
    parser.add_argument("--baseline", default="serial,fifo",
                        help=f"逗号分隔，可选 {','.join(VALID_BASELINES)}")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--out", type=Path, help="输出 JSON 路径；缺省按时间戳写入 results/")
    args = parser.parse_args()

    baselines = [b.strip() for b in args.baseline.split(",")]
    bad = [b for b in baselines if b not in VALID_BASELINES]
    if bad:
        parser.error(f"--baseline 只能使用 {','.join(VALID_BASELINES)}，收到 {bad}")
    if args.repeat < 1:
        parser.error("--repeat 必须 > 0")

    runs = []
    for baseline in baselines:
        for repeat in range(1, args.repeat + 1):
            entry = run_once(baseline)
            entry["repeat"] = repeat
            runs.append(entry)

    results = {
        "meta": {
            "os": platform.platform(),
            "kernel": platform.release(),
            "python": platform.python_version(),
            "seed": SEED,
            "repeat": args.repeat,
            "data_source": "runtime_v1_v2_real",
            "load_source": "bench/scenario.py TASKS",
            "note": "调度开销为真实 CPU/墙钟占比（S1 占位口径）；峰值内存用 tracemalloc",
        },
        "runs": runs,
    }

    results_dir = _ROOT / "results"
    results_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    output_path = args.out or results_dir / f"s1-{timestamp}.json"
    if output_path.exists():
        parser.error(f"结果文件已存在，原始 JSON 禁止覆盖：{output_path}")
    with output_path.open("x", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
        f.write("\n")

    # 终端摘要（供截图：命令与输出同框）
    print(f"已保存 {len(runs)} 条记录：{output_path}")
    for baseline in baselines:
        vals = [r["metrics"]["makespan_ms"] for r in runs if r["baseline"] == baseline]
        serial_val = next((r["metrics"]["makespan_ms"] for r in runs
                           if r["baseline"] == "serial"), None)
        line = f"  {baseline:>8}: makespan 各次 = {vals}"
        if serial_val:
            line += f" | 相对 serial 加速比 = {serial_val / (sum(vals) / len(vals)):.2f}x"
        print(line)


if __name__ == "__main__":
    main()
