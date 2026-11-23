"""生成 S1 绘图练习用的模拟结果 JSON。"""

import argparse
import json
import platform
from datetime import datetime
from pathlib import Path

from scenario import TASKS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", default="serial,naive")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    baselines = args.baseline.split(",")
    if any(baseline not in {"serial", "naive", "runtime"} for baseline in baselines):
        parser.error("--baseline 只能使用 serial、naive 或 runtime")
    if args.repeat < 1:
        parser.error("--repeat 必须大于 0")

    # 暂用任务耗时之和作为示例值，供绘图脚本读取。
    example_makespan = sum(task["duration_ms"] for task in TASKS)
    runs = []

    for baseline in baselines:
        for repeat in range(1, args.repeat + 1):
            runs.append(
                {
                    "baseline": baseline,
                    "load": "abc_stub",
                    "repeat": repeat,
                    "metrics": {
                        "makespan_ms": example_makespan,
                        "copy_count": 0,
                        "copy_bytes": 0,
                        "peak_rss_mb": 0,
                        "sched_overhead_pct": 0,
                        "recovery_ms": None,
                        "affected_tasks": None,
                    },
                }
            )

    results = {
        "meta": {
            "os": platform.platform(),
            "kernel": platform.release(),
            "python": platform.python_version(),
            "seed": 20261111,
            "repeat": args.repeat,
            "data_source": "stub_example",
        },
        "runs": runs,
    }

    results_dir = Path(__file__).resolve().parent.parent / "results"
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    output_path = args.out or results_dir / f"stub-{timestamp}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with output_path.open("x", encoding="utf-8") as output_file:
            json.dump(results, output_file, ensure_ascii=False, indent=2)
            output_file.write("\n")
    except FileExistsError:
        parser.error(f"结果文件已存在，请换一个输出文件名：{output_path}")

    print(f"已保存 {len(runs)} 条模拟记录：{output_path.resolve()}")


if __name__ == "__main__":
    main()