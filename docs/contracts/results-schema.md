# 契约②：results.json 字段表（草稿）

## 1. 结果结构

`results.json` 顶层包含 `meta` 和 `runs`。

| 字段 | 含义 |
|---|---|
| `meta.os` | 实验环境的操作系统版本 |
| `meta.kernel` | 内核版本 |
| `meta.python` | Python 版本 |
| `meta.seed` | 随机种子，固定为 `20261111` |
| `meta.repeat` | 每组实验的重复次数，固定为 `3` |
| `runs` | 逐次运行的结果列表，每次运行保留一条记录 |
| `runs[].baseline` | 本次运行的模式，取 `serial`、`naive` 或 `runtime` |
| `runs[].load` | 本次运行使用的负载 |
| `runs[].repeat` | 本次运行是这一组的第几次重复 |
| `runs[].metrics` | 本次运行的指标，字段见下表 |

## 2. 七项指标

| 指标 | 字段名 | 定义 | 采集点 | 单位 |
|---|---|---|---|---|
| 总完成时间 | `makespan_ms` | 最后一个任务终止时间减去首个任务开始时间 | 调度器时间戳 | 毫秒 |
| 上下文拷贝次数 | `copy_count` | 真实发生的对象复制事件数 | `ContextStore` 的写入复制路径 | 次 |
| 上下文拷贝字节数 | `copy_bytes` | 所有被复制对象的字节数之和 | `ContextStore` 的写入复制路径 | 字节 |
| 峰值内存 | `peak_rss_mb` | 实验期间采到的最大常驻内存 | 采样线程通过 `resource.getrusage` 获取 | MB |
| 调度开销 | `sched_overhead_pct` | 调度器自身耗时除以总完成时间，再乘以 100 | 调度器计时器 | 百分比 |
| 故障恢复时间 | `recovery_ms` | 从注入失败到受影响任务重新就绪的时间 | `fault.py` 日志 | 毫秒 |
| 受影响任务数 | `affected_tasks` | 因单个任务故障而未能按时完成的任务数 | 调度器统计 | 个 |

## 3. S1 记录与归档规则

- S1 用 stub 模拟数据跑通脚本、JSON 输出和绘图流程。
- `recovery_ms` 和 `affected_tasks` 从 S2 开始采集。S1 草稿约定两个字段留空，在 JSON 中用 `null` 表示本轮未采集。
- 第二条基线的字段值统一为 `naive`，图例写 `naive (FIFO)`。
- CLI 参数口径为 `--baseline serial,naive,runtime --repeat 3 --out results.json`。
- 每组三次重复的原始值都保存到 `results/`。重跑时另存，原始 JSON 不覆盖、不手改。

## 4. 对接待确认

指标核对清单把两个拷贝指标的采集点写为 `ContextStore.copy()`，契约①则把计数日志写在 `ContextStore.write()` 的复制分支。草稿先写成「写入复制路径」，接入真实运行时前由 T2 确认具体方法。
S1 两个故障指标暂用 `null` 表示未采集，待队友确认脚本是否统一采用这一写法。