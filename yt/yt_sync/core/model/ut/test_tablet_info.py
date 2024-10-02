from typing import Any

import pytest

from yt.yt_sync.core.model.tablet_info import YtTabletInfo


@pytest.mark.parametrize("tablet_count", [0, None])
@pytest.mark.parametrize("pivot_keys", [None, [], [[]]])
def test_no_tablets(tablet_count: int | None, pivot_keys: list[Any] | None):
    tablet_info = YtTabletInfo.make(tablet_count, pivot_keys, dict())
    assert not tablet_info.has_pivot_keys
    assert not tablet_info.has_tablets


def test_has_tablets_by_tablet_count():
    tablet_info = YtTabletInfo.make(1, None, dict())
    assert not tablet_info.has_pivot_keys
    assert tablet_info.has_tablets


def test_has_tablets_by_pivot_keys():
    tablet_info = YtTabletInfo.make(None, [[1]], dict())
    assert tablet_info.has_pivot_keys
    assert tablet_info.has_tablets


def test_is_resharding_required_by_tablet_count():
    actual = YtTabletInfo.make(1, None, dict())
    desired = YtTabletInfo.make(2, None, dict())
    assert actual.is_resharding_required(desired)


def test_is_resharding_required_by_pivot_keys():
    actual = YtTabletInfo.make(None, [[1]], dict())
    desired = YtTabletInfo.make(None, [[1], [2]], dict())
    assert actual.is_resharding_required(desired)


def test_no_resharding_required_by_tablet_balancer_settings():
    actual = YtTabletInfo.make(1, None, dict())
    desired = YtTabletInfo.make(2, None, {"enable_auto_reshard": True})
    assert not actual.is_resharding_required(desired)
