#!/usr/bin/env bash
# 一键复现脚本（T2 陈苗苗，2026-11-24）—— 本周规划 11/23「一键复现脚本核对」
# 用法：bash run_all.sh   （openEuler / Linux 与 Windows Git Bash 均可）
# 步骤：建 venv -> 装锁定依赖 -> 全量单测 -> 三基线首轮实验 -> JSON 合法性校验
set -e
cd "$(dirname "$0")"

echo "== [1/5] 建虚拟环境 =="
python -m venv .venv
if [ -f .venv/bin/activate ]; then source .venv/bin/activate; else source .venv/Scripts/activate; fi

echo "== [2/5] 安装锁定依赖（requirements.lock）=="
pip install -q -r requirements.lock

echo "== [3/5] 全量单测 =="
python -m pytest -q tests/

echo "== [4/5] 三基线首轮实验（serial,fifo,runtime x repeat 3）=="
python bench/bench.py --baseline serial,fifo,runtime --repeat 3

echo "== [5/5] JSON 合法性校验 =="
LATEST=$(ls -t results/s1-*.json | head -1)
python -m json.tool "$LATEST" > /dev/null && echo "JSON 合法：$LATEST"
echo "一键复现完成。"
