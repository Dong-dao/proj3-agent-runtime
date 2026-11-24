@echo off
rem 一键复现脚本（Windows，T2 陈苗苗，2026-11-24）—— 对应 run_all.sh
cd /d %~dp0

echo == [1/5] 建虚拟环境 ==
python -m venv .venv
call .venv\Scripts\activate.bat

echo == [2/5] 安装锁定依赖 ==
pip install -q -r requirements.lock

echo == [3/5] 全量单测 ==
python -m pytest -q tests/
if errorlevel 1 exit /b 1

echo == [4/5] 三基线首轮实验 ==
python bench\bench.py --baseline serial,fifo,runtime --repeat 3

echo == [5/5] JSON 合法性校验 ==
for /f "delims=" %%i in ('dir /b /o-d results\s1-*.json') do (
    python -m json.tool results\%%i > nul && echo JSON 合法：results\%%i
    goto :done
)
:done
echo 一键复现完成。
