"""画像报告的冻结存储：唯一身份、租约保护与原子发布。

磁盘布局（独立于 data/，GC 完全不碰这个目录）：

    <root>/
      quality_profiles/
        LOCK                                跨进程互斥锁（flock）
        reports/revNNNNNN-<digest12>.json  已冻结报告（只读，永不改写）
        .tmp-<random>                       发布前的临时文件（崩溃残骸可清）

身份与不可变性：

  - 报告唯一身份 = 数据修订号（revision）+ 规则摘要（rule_digest）；
  - 重复请求直接返回已冻结的原报告；换一组规则得到不同摘要，写另一个
    文件，绝不会覆盖既有报告；
  - 报告发布后文件权限置为只读；所有写入都走“同目录临时文件 + fsync +
    os.replace + 目录 fsync”，进程在正式发布前退出，外界只能看到
    临时残骸，看不到任何不完整报告，重复请求会重新计算并恢复。

读者保护：统计期间持有 table.lease(version)，清理任务（gc.collect）
不会删除该修订号引用的输入文件；报告保存完成后内容自包含，即使
原始数据文件之后被整理或回收，报告仍可打开与比较。
"""

from __future__ import annotations

import json
import os
import tempfile
import time

from .. import manifest as mf
from .. import storage
from ..errors import ProfileNotFoundError
from .profile import compute_profile
from .rules import normalize_rules, rules_digest

PROFILES_DIRNAME = "quality_profiles"
REPORTS_DIRNAME = "reports"

# 测试钩子：命中后在对应位置以 os._exit 硬退出（真正退出的子进程）
#   prepared:  进入临界区、确认需要新报告后、开始计算前退出
#   computed:  计算完成、临时文件发布前退出
#   staged:    临时文件已写完 fsync、os.replace 之前退出（模拟发布边界中断）
CRASH_ENV = "LAKE_QUALITY_CRASH_POINT"


def _maybe_crash(point: str) -> None:
    if os.environ.get(CRASH_ENV) == point:
        os._exit(23)


class ProfileStore:
    def __init__(self, table, profiles_root: str | None = None):
        self.table = table
        if profiles_root is None:
            profiles_root = os.path.join(table.root, PROFILES_DIRNAME)
        self.profiles_root = os.path.abspath(profiles_root)
        self.reports_dir = os.path.join(self.profiles_root, REPORTS_DIRNAME)
        self.lock_path = os.path.join(self.profiles_root, "LOCK")

    def _ensure_dirs(self) -> None:
        os.makedirs(self.reports_dir, exist_ok=True)

    def _lock(self) -> storage.FileLock:
        self._ensure_dirs()
        return storage.FileLock(self.lock_path)

    def report_path(self, revision: int, digest: str) -> str:
        return os.path.join(
            self.reports_dir, f"rev{revision:06d}-{digest[:12]}.json"
        )

    # ------------------------------------------------------------- 清理
    def _cleanup_stale_tmp(self, now: float, max_age: float) -> list[str]:
        """清理发布前崩溃留下的临时文件残骸（按年龄保护在途写入）。"""
        removed = []
        for name in sorted(os.listdir(self.profiles_root)):
            if not name.startswith(".tmp-"):
                continue
            p = os.path.join(self.profiles_root, name)
            if os.path.isfile(p) and now - os.path.getmtime(p) >= max_age:
                os.unlink(p)
                removed.append(name)
        return removed

    # ------------------------------------------------------------- 创建
    def create_or_get(
        self,
        raw_rules,
        version: int | None = None,
        batch_size: int | None = None,
        now: float | None = None,
    ) -> dict:
        """按（修订号, 规则摘要）身份创建或取回冻结报告。

        规则格式错误 -> RuleSpecError，且不写任何文件。
        已存在 -> 原样返回（reused=True），绝不重新计算、绝不覆盖。
        不存在 -> 持读者租约完成统计，原子发布后返回（reused=False）。
        """
        # 规则规范化在任何 IO 之前完成：错误配置不可能落盘任何报告
        rules = normalize_rules(raw_rules)
        digest = rules_digest(rules)

        self._ensure_dirs()
        now = time.time() if now is None else now
        with self._lock():
            target = self.current_target_version(version)
            path = self.report_path(target, digest)
            if os.path.exists(path):
                report = self._read(path)
                report["reused"] = True
                return report

            # 目标报告确认不存在：本目录里的 .tmp 只能是此前进程在发布
            # 边界崩溃留下的残骸（flock 保证当前没有其他在途发布），
            # 立即清理，使“相同请求恢复”不依赖年龄阈值。
            self._cleanup_stale_tmp(now, max_age=0.0)

            _maybe_crash("prepared")

            # 租约在计算期间保护目标修订号的全部输入文件；与 GC 共用
            # 表的提交锁，GC 不可能在扫描途中删掉受保护文件。
            with self.table.lease(target):
                payload = compute_profile(
                    self.table,
                    target,
                    rules,
                    batch_size=batch_size or 1000,
                )
            payload["frozen_at"] = time.time()
            _maybe_crash("computed")
            self._publish_atomic(path, payload)

        report = self._read(path)
        report["reused"] = False
        return report

    def current_target_version(self, version: int | None) -> int:
        if version is None:
            return mf.current_version(self.table.meta_root)
        v = int(version)
        if not os.path.exists(mf.version_path(self.table.meta_root, v)):
            raise ProfileNotFoundError(f"修订号 {v} 不存在")
        return v

    def _publish_atomic(self, path: str, payload: dict) -> None:
        """临时文件 + fsync + 原子 rename + 目录 fsync，随后置只读。

        临时文件放在 profiles 根目录（与 reports/ 不同目录），因此任何
        崩溃残骸都不可能出现在正式报告目录里，list/扫描报告目录只见
        已发布文件；rename 在同一文件系统内仍是原子的。
        """
        directory = os.path.dirname(path)
        fd, tmp = tempfile.mkstemp(dir=self.profiles_root, prefix=".tmp-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, sort_keys=True, ensure_ascii=False)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            if os.environ.get(CRASH_ENV) == "staged":
                os._exit(23)  # 临时文件已落盘、原子 rename 之前硬退出
            os.replace(tmp, path)
            storage.fsync_dir(directory)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise
        os.chmod(path, 0o444)  # 冻结：只读，任何后续写入/整理都不能改写

    # ------------------------------------------------------------- 读取
    @staticmethod
    def _read(path: str) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def get(self, version: int, digest: str) -> dict:
        path = self.report_path(version, digest)
        if not os.path.exists(path):
            raise ProfileNotFoundError(
                f"无画像报告: revision={version}, rule_digest={digest[:12]}"
            )
        return self._read(path)

    def get_by_rules(self, version: int, raw_rules) -> dict:
        rules = normalize_rules(raw_rules)
        return self.get(version, rules_digest(rules))

    def list_reports(self) -> list[dict]:
        """列出全部已冻结报告的身份信息（不读数据文件，报告自包含）。"""
        out = []
        if not os.path.isdir(self.reports_dir):
            return out
        for name in sorted(os.listdir(self.reports_dir)):
            if not name.endswith(".json"):
                continue
            rep = self._read(os.path.join(self.reports_dir, name))
            out.append(
                {
                    "report_id": rep["report_id"],
                    "revision": rep["revision"],
                    "rule_digest": rep["rule_digest"],
                    "rule_summary": rep["rule_summary"],
                    "passed": rep["passed"],
                    "frozen_at": rep.get("frozen_at"),
                    "path": os.path.join(self.reports_dir, name),
                }
            )
        return out

    def find(self, version: int, rule_summary: str | None = None) -> list[dict]:
        """按修订号（可选再按规则摘要文本）查找已冻结报告。"""
        reps = [r for r in self.list_reports() if r["revision"] == version]
        if rule_summary is not None:
            reps = [r for r in reps if r["rule_summary"] == rule_summary]
        return reps
