# 小型湖表压实引擎 — 架构说明

面向离线训练集整理场景：大量碎片 Parquet 需要合并、位置删除需要物化。
纯本机文件系统 + 进程锁实现，不依赖对象存储，也不兼容 Iceberg。

## 1. 磁盘布局

```
<root>/
  data/                        已发布文件（不可变）
    data-<data_file_id>.parquet     数据文件，含稳定 row_id 列
    deletes-<delete_file_id>.parquet 位置删除文件
  staging/                     未发布的压实候选（不在读路径上）
    compact-<plan_id>/
      data-<new_id>.parquet         合并后的候选数据文件
      plan.json                     计划：基版本、选中文件、位置映射
  metadata/
    versions/vNNNNNN.json      不可变 manifest，一版一个文件
    HEAD                       当前版本号（原子替换）
    LOCK                       flock 跨进程提交锁
    leases/lease-<id>.json     读者租约
```

**Manifest** 完整描述一个表版本：

```json
{
  "version": 9, "parent": 8, "created_at": ...,
  "data_files":  {"<id>": {"path": "data/data-<id>.parquet", "num_rows": 19}},
  "delete_files": {"<id>": {"path": "data/deletes-<id>.parquet",
                            "target": "<data_file_id>", "num_deletes": 1}}
}
```

**位置删除文件**内每行显式记录 `(data_file_id, pos)`，`pos` 是该数据文件内
**从零开始的物理行号**。约束：一个删除文件只针对一个数据文件，压实时才能
按文件整体“吸收”或“翻译”。行号只来自读取时的物理位置，**绝不**从
`row_id` 等业务字段反推。

## 2. 原子发布点

所有推进版本的操作（追加、发布删除、提交压实）走同一条路径：

1. 新数据/删除文件先以临时名写入 `data/` 并 `os.replace` 就位，fsync 目录；
2. 新 manifest 写入 `versions/vNNNNNN.json`（临时文件 + fsync + rename）；
3. 最后原子替换 `HEAD`（rename 在同一文件系统内是原子的）。

读者先读 `HEAD` 固定版本号，再读对应 manifest。manifest 先于 HEAD 就位，
因此读者要么看到旧版本、要么看到完整的新版本——这就是跨进程原子发布点。
`staging/` 里的候选不被任何 manifest 引用，天然不在读路径上。
所有写者共用 `metadata/LOCK` 上的 `flock` 排他锁，版本号单调递增。

## 3. 扫描（读路径）

`scan(version)`：固定 manifest → 读出其中全部删除文件，按 `data_file_id`
归组成位置集合 → 用 PyArrow 按物理顺序读每个数据文件，按位置过滤
（`pyarrow.parquet` 保证按文件物理行序返回，与删除项的 0 起行号严格对应）。

位置过滤刻意用 PyArrow 而非 SQL：物理行序是格式层保证，不该交给查询引擎
的扫描顺序假设。DuckDB 用于内容级校验（`lake/verify.py` 把 Arrow 表注册
进去做 `GROUP BY row_id` 计数，比对多重集）。

## 4. 压实

**生成计划（`create_plan`）**：在基版本 V 选中小文件（显式列表或
`max_rows` 阈值），剔除 V 已记录的删除行，把活行合并成一个候选 Parquet
写入 `staging/compact-<plan_id>/`，并记录每个旧文件的
`旧物理行号 -> 新物理行号` 映射与被吸收的删除位置。计划落盘为 `plan.json`。

**提交计划（`commit_plan`）**：在提交锁下把当前版本 W 与基版本 V 比较——

| 情况 | 处理 |
|---|---|
| 选中文件在 W 中已不存在（被别的压实替换） | `ConflictError`，计划标记 `failed`，不覆盖对方 |
| W 相对 V 的新增数据文件（并发追加） | 原样保留（随 W 的 manifest 自然带入） |
| W 相对 V 新到的、针对选中文件的删除 | 按映射翻译到新文件位置，写成新删除文件随新版本发布 |
| V 已有的针对选中文件的删除 | 行已被物理剔除，删除文件被“吸收”丢弃 |
| 针对未选中文件的删除 | 原样保留 |

随后组装新 manifest 并翻转 HEAD，staging 目录即清理。

## 5. 读者租约与垃圾回收

- `table.lease(version)`：在提交锁下写入 `leases/lease-<id>.json`。
  租约与 GC 共用同一把锁，GC 不会漏看刚创建的租约。
- `collect()`：受保护版本 = 当前 HEAD ∪ 所有存活租约的版本。
  只有受保护版本引用的数据/删除文件允许保留，其余删除；
  `staging/` 下超过 `max_age` 的候选目录（崩溃残留、失败计划）一并清除。
- manifest JSON 本身不裁剪（体量小，且进行中的压实计划需要读基版本）。

## 6. 崩溃恢复

- 追加/发布删除中途崩溃：`data/` 里留下不被任何 manifest 引用的孤儿文件，GC 回收。
- 压实提交前崩溃：候选留在 `staging/`，不进读路径，GC 按年龄清理。
- 提交在 HEAD 翻转前崩溃：版本未发布，与从未发生无异；翻转后崩溃：
  版本已完整发布，staging 残骸由 GC 清理。

## 7. 限制（本题范围）

- 仅支持同一台机器上的多进程并发（flock 语义决定）。
- staging 清理依赖年龄阈值，进行中的压实须在阈值内完成（生产上可换计划心跳）。
- manifest 只增不减；如需裁剪历史，需保证无进行中计划引用基版本。

## 8. 运行

```bash
./run.sh        # 自动化校验（pytest）+ 叙述式演示（demo.py）
```

测试实际读写 Parquet、实际 fork 子进程，用同步屏障精确安排
“计划生成—额外删除—提交”的交错，并用 DuckDB 核对 row_id 多重集。
