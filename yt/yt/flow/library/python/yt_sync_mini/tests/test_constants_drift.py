"""Drift guard: yt_sync_mini's constants must stay identical to yt_sync's.

The opensource shim copies queue/consumer/producer constants from
``yt/yt_sync/core/constants``. This test compares the copies against the
original; it is skipped in builds where yt_sync is absent.
"""

import pytest

import yt.yt.flow.library.python.yt_sync_mini as mini

yt_sync_constants = pytest.importorskip("yt.yt_sync.core.constants")

if getattr(yt_sync_constants, "QUEUE_META_COLUMNS", None) is mini.QUEUE_META_COLUMNS:
    pytest.skip(
        "yt.yt_sync.core.constants is the yt_sync_mini alias — nothing to compare against",
        allow_module_level=True,
    )


@pytest.mark.parametrize(
    "name",
    [
        "QUEUE_META_COLUMNS",
        "CONSUMER_SCHEMA",
        "CONSUMER_ATTRS",
        "PRODUCER_SCHEMA",
        "PRODUCER_ATTRS",
    ],
)
def test_constant_matches_yt_sync(name):
    assert getattr(mini, name) == getattr(yt_sync_constants, name), (
        f"{name} drifted from yt/yt_sync/core/constants; " "update yt/yt/flow/library/python/yt_sync_mini/constants.py"
    )
