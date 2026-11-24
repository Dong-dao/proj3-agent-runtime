"""runtime 包：proj3 Agent Runtime 内核层（T2 · 陈苗苗）。

七个模块对应《设计理念》3.4 节的机制映射；S1 范围严格按本周规划执行：
状态机（agent）、依赖 DAG（graph）、调度器（scheduler）、统一句柄与资源
（handle / resource）为实现模块；COW 上下文（context）、故障域（fault）、
消息总线（bus）、可观测（observer）S1 仅留骨架，S2 再实现。
"""
