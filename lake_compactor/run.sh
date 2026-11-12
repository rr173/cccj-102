#!/usr/bin/env bash
# 一键运行：自动化校验（pytest）+ 叙述式演示（demo.py / demo_quality.py / demo_split.py）
set -euo pipefail
cd "$(dirname "$0")"

# 选择带有 pyarrow/duckdb/pytest 的解释器
if [ -n "${PYTHON:-}" ]; then
  PY="$PYTHON"
elif python3 -c "import pyarrow, duckdb, pytest" 2>/dev/null; then
  PY=python3
elif [ -x /workspace/.venv/bin/python3 ]; then
  PY=/workspace/.venv/bin/python3
else
  echo "未找到 pyarrow/duckdb/pytest，请先安装或用 PYTHON=/path/to/python 指定" >&2
  exit 1
fi

echo "==> 使用解释器: $PY"
echo "==> [1/8] 自动化校验（pytest，真实读写 Parquet、真实多进程交错、定点崩溃恢复）"
"$PY" -m pytest -v
echo "==> [2/8] 压实/摄取端到端演示（demo.py）"
"$PY" demo.py
echo "==> [3/8] 数据质量画像自动化校验（tests/test_quality.py，含可手算样本）"
"$PY" -m pytest tests/test_quality.py -v
echo "==> [4/8] 数据质量画像端到端演示（demo_quality.py）"
"$PY" demo_quality.py
echo "==> [5/8] 成组抽样切分自动化校验 + 端到端演示（tests/test_split.py / demo_split.py）"
"$PY" -m pytest tests/test_split.py -v
"$PY" demo_split.py
echo "==> [6/8] 查询工作台自动化校验 + 端到端演示（tests/test_workbench.py / demo_workbench.py）"
"$PY" -m pytest tests/test_workbench.py -v
"$PY" demo_workbench.py
echo "==> [7/8] 实验分支自动化校验 + 端到端演示（tests/test_branches.py / demo_branches.py）"
"$PY" -m pytest tests/test_branches.py -v
"$PY" demo_branches.py
echo "==> [8/8] 字段身份与结构演进自动化校验 + 端到端演示（tests/test_schema.py / demo_schema.py）"
"$PY" -m pytest tests/test_schema.py -v
"$PY" demo_schema.py
echo "==> 全部完成"
