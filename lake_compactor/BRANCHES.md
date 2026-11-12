# 实验分支（branches）

训练团队在正式数据上做样本修订试验时，不能直接追加或删行（会影响正式
读者），也不必复制整套目录（浪费空间）。实验分支用同一份底层内容文件
支撑多条命名提交线：

- `main` = 正式数据，尖端就是原有的 `metadata/HEAD`。
- 实验分支 = `metadata/branches.json` 里的 `name -> 提交号` 引用；建分支
  不复制任何数据文件或 manifest，只是多一个尖端。
- 提交号是**全库唯一坐标**（所有已落盘版本号的最大值 + 1），每条提交的
  `parent` 指向本线前驱，`branch` 记录作者线。

旧格式目录没有 `branches.json`，第一次使用时自然只有 `main`；所有原有
调用（`LakeTable.open(root)`、`append`、`scan`、`ingest_batch`、
`create_split`、工作台按版本号取数等）无需改为传分支参数。

## Python 接口

```python
from lake import LakeTable

t = LakeTable.open("./mylake")               # 绑定 main（正式数据）
t.list_branches()                            # [{'name','tip','parent',...}, ...]

info = t.create_branch("exp-2026w46")        # 从当前 main 尖端建立
info = t.create_branch("from-old", from_version=12)   # 从任意已有提交建立

e = t.checkout("exp-2026w46")                # 切换访问对象（同一物理目录）
e.branch                                     # 'exp-2026w46'
e.append([("sample-1", 1)])                  # 原有读写能力直接在 e 上工作
e.publish_deletes(fid, [0])
e.ingest_batch("key", messages)
e.scan()                                     # 只看到本线的可见行
e.ingest_cursors()                           # 本线游标
e.batch_record("key")                        # 本线批次记录，别的线不可见

e.publish(expected_main_version=t.current_version())   # 快进交付给 main
e.delete_branch("exp-2026w46")               # 撤销实验分支
t.delete_branch("main")                      # ProtectedBranchError：main 不能撤销
```

`LakeTable.open(root, branch="name")` 可直接打开某条分支；名字不存在抛
`BranchNotFoundError`。

## 名字规则

合法名字：字母/数字开头，长度 1–64，只含 `A-Z a-z 0-9 . _ -`，不以 `.`
开头或结尾，不使用引擎保留前缀（`metadata` / `data` / `staging` /
`splits` / `quality_profiles`）。

- 非法名字 → `BranchNameError`；
- 已占用或与保留名 `main` 冲突（含大小写变体）→ `BranchExistsError`；
- 建分支的基提交不存在 → `BranchBaseError`；
- 撤销 `main` → `ProtectedBranchError`；撤销不存在分支 →
  `BranchNotFoundError`。

全部拒绝都发生在写任何文件之前（名字校验在取得提交锁之前完成）。

## 写入隔离

`append` / `publish_deletes` / `ingest_batch` / `commit_plan` 只读、只写
**绑定分支的尖端**：

- `_next()` 在提交锁内以本线尖端为父，版本号取全库最大 + 1；
- `_publish()` 再核验一次父提交等于本线尖端，否则 `ConflictError`
  （同一分支内的竞争仍须正确仲裁，绝不悄悄重排或倒退尖端）；
- 另一条线推进版本不构成本线冲突；
- 压实计划记录生成时的 `base_branch`，在另一条分支提交计划 →
  `BranchMismatchError`（不能借用别线的整理成果）。

读取默认解析绑定分支的尖端；显式 `version=` 则是全库坐标中的那个提交，
可以读取任何分支的历史快照。

## 微批接入的分支隔离

游标和批次索引都随 manifest 携带，因此天然按分支独立：两条线从各自父
提交继承进度，各自推进。另外：

- receipt = `HASH(batch_key, 内容摘要, 分支名)`（`main` 与旧实现逐字节
  兼容），同一请求键与内容在两条线上得到不同回执，不能跨线借用；
- 批次记录文件名带分支摘要，同 key 的两条线不会覆盖彼此的审计载体；
- 幂等重放只认本线尖端 manifest 的批次索引：另一条线的同名 key 不
  会让本线命中重放，也不会触发幂等冲突。

## publish（把实验尖端交给 main）

```python
result = e.publish(expected_main_version=main_tip)
```

- 只做**快进**：要求 `main` 尖端是实验尖端的祖先（含同一提交）；成功后
  只把 `HEAD` 原子指向那个**已存在的实验提交**。
- 不写新 manifest、不复制任何数据文件、不产生内容相同的“镜像提交”；
  实验分支引用保留，可继续推进或撤销。
- 调用方必须给出预期的 main 尖端。先核对预期再核对祖先关系：
  - 预期过期（检查后又被别的正式写者推进）→ `StaleMainError`；
  - 两条线已分叉 → `DivergedError`，不自动内容合并，由人处理；
  - 在 main 自身上调用 → `SourceIsMainError`。
- 任何拒绝都保留双方原状：不移动尖端、不产生版本、不动数据文件。
- 预期为整数之外 → `PublishError(reason="invalid-expected-main")`。

## 回收语义

`collect()` 的保护集合 = **所有存活分支尖端的 manifest** ∪ 所有存活
读者租约指向的版本。不能仅凭 main 决定哪些底层内容可删：

- 实验尖端引用的独占文件在 main 上不可见，但只要分支还在就保留；
- 撤销分支只移除引用。其独占提交随后不再被任何存活尖端引用，若没有
  租约指向它们，下一次 `collect()` 才删除其数据/删除文件；
- 正在读取该内容的读者持有租约，租约版本仍在保护集合中，读取期间
  文件不会被删；读完后再次 `collect()` 才清除；
- 撤销后重新打开仍可见其它分支与 main 的全部引用。

## 目录与持久化

```
<root>/metadata/
  HEAD               main 尖端
  branches.json      {"branches": {"<name>": <tip>, ...}}（原子写）
  versions/v*.json   全库唯一编号的不可变提交（parent 记录提交关系）
  leases/            读者租约（含分支名，仅作审计）
```

`HEAD` 与 `branches.json` 都是"临时文件 + fsync + rename"原子替换；提交
manifest 先于尖端就位。进程崩溃后重开：分支尖端、提交关系、读数、游标、
回执全部一致（见 `tests/test_branches.py::test_reopen_preserves_branches_...`）。

## 演示与校验

```bash
python3 -m pytest tests/test_branches.py -v
python3 demo_branches.py
```

演示按验收顺序走：实验改动不可见 → 快进 publish → main 独立变更后发布
被拒 → 两条线并发接入相同请求键并各自重放 → 一条线整理同时另一条线修改
→ 读取撤销内容期间回收、读完再回收 → 关闭重开核对全部引用。
每一步都断言完整行内容（row_id/value）、manifest 文件集合、提交号/父
提交、游标、receipt 与磁盘文件存在性。
