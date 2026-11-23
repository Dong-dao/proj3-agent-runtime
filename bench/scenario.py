"""S1 首轮 stub 测验用的模拟任务。"""

TASKS = [
    {
        "task_id": "A",
        "depends_on": [],
        "duration_ms": 30,
    },
    {
        "task_id": "B",
        "depends_on": ["A"],
        "duration_ms": 20,
    },
    {
        "task_id": "C",
        "depends_on": [],
        "duration_ms": 10,
    },
]