"""读取测验结果，绘制总完成时间对比图。

S1 先用 stub 数据验证 JSON 到图表的流程。
项目基线包括 serial、naive 和 runtime。
当前脚本默认绘制 serial 与 naive，图例使用 naive (FIFO)。
S1 的 stub 图只用于流程验证，不能作为性能结论。
接入真实 Runtime 结果后，再将 runtime 纳入正式对比图。
"""

import argparse
import json
from pathlib import Path
from statistics import mean

import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_file", type=Path, help="要读取的结果 JSON")
    args = parser.parse_args()

    with args.results_file.open(encoding="utf-8") as file:
        results = json.load(file)

    baselines = ("serial", "naive")
    labels = ("serial", "naive (FIFO)")
    averages = []

    for baseline in baselines:
        times = [
            run["metrics"]["makespan_ms"]
            for run in results["runs"]
            if run["baseline"] == baseline
        ]
        if not times:
            parser.error(f"结果文件中没有 {baseline} 的记录")
        averages.append(mean(times))

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(labels, averages, color=["#376da5", "#e58d40"])
    ax.bar_label(bars, fmt="%.1f", padding=4)
    ax.set_ylim(0, max(averages) * 1.2 or 1)
    ax.set_xlabel("Execution mode")
    ax.set_ylabel("Mean makespan (ms)")
    ax.set_title("S1 stub example — A/B/C workload, 3 runs per mode")
    fig.tight_layout()

    figures_dir = Path(__file__).resolve().parent.parent / "figures"
    figures_dir.mkdir(exist_ok=True)
    output_path = figures_dir / f"{args.results_file.stem}-makespan.png"
    fig.savefig(output_path, dpi=150)
    plt.close(fig)

    print(f"已保存对比图：{output_path}")


if __name__ == "__main__":
    main()