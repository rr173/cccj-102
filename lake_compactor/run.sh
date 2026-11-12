#!/usr/bin/env bash
# 一键运行：自动化校验（pytest）+ 叙述式演示（demo.py）
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
echo "==> [1/2] 自动化校验（pytest，真实读写 Parquet、真实多进程交错、定点崩溃恢复）"
"$PY" -m pytest -v
echo "==> [2/2] 端到端演示（demo.py）"
"$PY" demo.py
echo "==> 全部完成"
