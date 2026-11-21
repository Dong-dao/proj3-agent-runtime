# CONTRIBUTING

## 一、分工认领（2026-11-16 定稿）

| 人 | 固定职责 | 主责 | 协作 |
| --- | --- | --- | --- |
| 董博文 | T0 契约与集成（队长，不占主责名额） | T1 机制设计 | T3 实验与数据 |
| 陈苗苗 | — | T2 Runtime 实现 | T1 机制设计 |
| 栾诗童 | — | T3 实验与数据 | T2 Runtime 实现 |

- T0：冻结契约、周会检查点、对外问询、范围控制、兜底。
- 协作不是挂名：每周固定给主责提供输入、参加互讲、主责顶不住时接上。
- 换认领必须周会说明并完成交接，不静默换；本文件同步更新。

## 二、commit 规范

前缀 + 一句说明，例：

- `contract: freeze interface v1`
- `design: S1 state machine and scheduling rationale`
- `feat(scheduler): v2 critical-path priority`
- `fix(agent): reject illegal transition on TERMINATED`
- `bench: first serial vs naive comparison`
- `docs: weekly report S1`

契约① 冻结后字段可加、语义不可改；改签名先在群里说清影响模块。

## 三、每周归档（周日前）

1. 设计草稿 → `docs/design/`（中文文件名，按周归档版本号）。
2. 运行截图 → `docs/screenshots/日期_内容.png`，命令与输出同框、带日期或版本信息。
3. 原始实验数据 → `results/`，禁止覆盖、禁止手改；只许追加。
4. 绘图输出 → `figures/`。
5. AI 使用记录 → `docs/AI工具使用台账.md` 当周条目。

## 四、AI 使用红线

- 章程允许 AI 辅助，但核心抽象、状态机、机制映射、实验设计必须本人完成并留 commit 与设计草稿。
- 台账如实填写：工具名称与版本、使用环节、主要提示词、生成比例与人工修改比例。不写 0%，也不夸大。
- 代码查重 + 文档查重两份都做；借鉴 AIOS 等须源码头部与文档双声明。

## 五、协作检查点

- 每周日晚互讲 30 分钟，互相挑一个「为什么这么做、换一种会怎样」。
- 协作人要能回答自己协作模块的设计依据与数据口径。
- 周报与风险清单由 T0 更新进腾讯文档与仓库。
