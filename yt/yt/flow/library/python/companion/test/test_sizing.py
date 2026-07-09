"""Tests for the companion-process-count sizing function and cgroup quota reader."""

import math

from yt.yt.flow.library.python.companion import sizing
from yt.yt.flow.library.python.companion.sizing import (
    DEFAULT_CAP,
    read_cpu_quota,
    resolve_companion_process_count,
)


def test_explicit_value_wins():
    assert resolve_companion_process_count(configured=4, cpu_quota=8.0) == 4


def test_explicit_value_is_capped():
    assert resolve_companion_process_count(configured=200, cpu_quota=8.0) == DEFAULT_CAP


def test_explicit_clamp_logs_warning(caplog):
    with caplog.at_level("WARNING", logger=sizing.__name__):
        assert resolve_companion_process_count(configured=200, cpu_quota=8.0) == DEFAULT_CAP
    assert any("clamping" in r.message.lower() or "exceeds the cap" in r.message for r in caplog.records)


def test_explicit_at_cap_does_not_warn(caplog):
    with caplog.at_level("WARNING", logger=sizing.__name__):
        assert resolve_companion_process_count(configured=DEFAULT_CAP, cpu_quota=8.0) == DEFAULT_CAP
    assert not any("clamping" in r.message.lower() or "exceeds the cap" in r.message for r in caplog.records)


def test_auto_uses_ceil_of_quota():
    assert resolve_companion_process_count(configured=0, cpu_quota=3.2) == 4


def test_auto_quota_exactly_one_core():
    assert resolve_companion_process_count(configured=0, cpu_quota=1.0) == 1


def test_auto_clamps_to_at_least_one_when_quota_zero():
    assert resolve_companion_process_count(configured=0, cpu_quota=0.0) == 1


def test_auto_clamps_to_at_least_one_when_quota_missing():
    assert resolve_companion_process_count(configured=0, cpu_quota=None) == 1


def test_auto_respects_default_cap():
    # A large finite quota clamps to the default cap.
    assert resolve_companion_process_count(configured=0, cpu_quota=999.0) == DEFAULT_CAP


def test_auto_respects_explicit_cap():
    assert resolve_companion_process_count(configured=0, cpu_quota=999.0, cap=64) == 64


def test_unlimited_quota_resolves_to_one():
    # Regression guard for YTFLOW-608: an unlimited (inf) quota — the common CI/dev
    # container case where no CPU limit is set — must NOT fan out to the cap. Forking
    # cap heavyweight companion interpreters there exhausted the thread budget and
    # aborted the worker. "No limit" means "do not fan out".
    assert resolve_companion_process_count(configured=0, cpu_quota=math.inf, cap=64) == 1
    assert resolve_companion_process_count(configured=0, cpu_quota=math.inf) == 1


def test_default_cap_is_sixteen():
    assert DEFAULT_CAP == 16
    assert resolve_companion_process_count(configured=0, cpu_quota=100.0) == 16


# --- read_cpu_quota cgroup parsing -------------------------------------------------


def _quota_with_files(monkeypatch, files):
    """Patch read_cpu_quota's ``open`` so only ``files`` (path -> content) exist."""
    real_open = open

    def fake_open(path, *args, **kwargs):
        if path in files:
            import io

            return io.StringIO(files[path])
        raise FileNotFoundError(path)

    monkeypatch.setattr(sizing, "open", fake_open, raising=False)
    try:
        return read_cpu_quota()
    finally:
        monkeypatch.setattr(sizing, "open", real_open, raising=False)


def test_read_cpu_quota_v2_finite(monkeypatch):
    quota = _quota_with_files(monkeypatch, {"/sys/fs/cgroup/cpu.max": "200000 100000\n"})
    assert quota == 2.0


def test_read_cpu_quota_v2_unlimited(monkeypatch):
    quota = _quota_with_files(monkeypatch, {"/sys/fs/cgroup/cpu.max": "max 100000\n"})
    assert math.isinf(quota)


def test_read_cpu_quota_v1_finite(monkeypatch):
    quota = _quota_with_files(
        monkeypatch,
        {
            "/sys/fs/cgroup/cpu/cpu.cfs_quota_us": "150000\n",
            "/sys/fs/cgroup/cpu/cpu.cfs_period_us": "100000\n",
        },
    )
    assert quota == 1.5


def test_read_cpu_quota_v1_unlimited(monkeypatch):
    quota = _quota_with_files(
        monkeypatch,
        {
            "/sys/fs/cgroup/cpu/cpu.cfs_quota_us": "-1\n",
            "/sys/fs/cgroup/cpu/cpu.cfs_period_us": "100000\n",
        },
    )
    assert math.isinf(quota)


def test_read_cpu_quota_unreadable_returns_none(monkeypatch):
    assert _quota_with_files(monkeypatch, {}) is None


def test_read_cpu_quota_v2_nested_cgroup(monkeypatch):
    # Host without cgroup namespaces: the mount-root cpu.max reads "max" while the real
    # limit lives in this process's own nested cgroup from /proc/self/cgroup.
    quota = _quota_with_files(
        monkeypatch,
        {
            "/proc/self/cgroup": "0::/a/b\n",
            "/sys/fs/cgroup/a/b/cpu.max": "max 100000\n",
            "/sys/fs/cgroup/a/cpu.max": "400000 100000\n",
            "/sys/fs/cgroup/cpu.max": "max 100000\n",
        },
    )
    assert quota == 4.0


def test_read_cpu_quota_v2_nested_takes_tightest_limit(monkeypatch):
    # The effective quota is the minimum over the cgroup path, not the deepest entry.
    quota = _quota_with_files(
        monkeypatch,
        {
            "/proc/self/cgroup": "0::/a\n",
            "/sys/fs/cgroup/a/cpu.max": "800000 100000\n",
            "/sys/fs/cgroup/cpu.max": "200000 100000\n",
        },
    )
    assert quota == 2.0


def test_read_cpu_quota_v1_nested_cgroup(monkeypatch):
    quota = _quota_with_files(
        monkeypatch,
        {
            "/proc/self/cgroup": "12:memory:/other\n4:cpu,cpuacct:/slot\n",
            "/sys/fs/cgroup/cpu/slot/cpu.cfs_quota_us": "300000\n",
            "/sys/fs/cgroup/cpu/slot/cpu.cfs_period_us": "100000\n",
            "/sys/fs/cgroup/cpu/cpu.cfs_quota_us": "-1\n",
            "/sys/fs/cgroup/cpu/cpu.cfs_period_us": "100000\n",
        },
    )
    assert quota == 3.0
