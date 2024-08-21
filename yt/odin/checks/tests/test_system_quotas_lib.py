import mock
import datetime

from yt.wrapper import YtClient

from yt_odin_checks.lib.system_quotas import get_cluster_name
from yt_odin_checks.lib.system_quotas import build_link_to_account
from yt_odin_checks.lib.system_quotas import build_link_to_bundle
from yt_odin_checks.lib.system_quotas import get_quota_holder_threshold
from yt_odin_checks.lib import system_quotas


def test_get_cluster_name():
    fake_yt_client = YtClient(proxy="fake.yt.yandex.net")
    assert get_cluster_name(fake_yt_client) == "fake"


def test_build_link_to_account():
    cluster = "fake"
    account = "tmp"
    account_link = build_link_to_account(cluster, account)
    assert account_link == "https://yt.yandex-team.ru/{}/accounts/general?account={}".format(cluster, account)


def test_build_link_to_bundle():
    cluster = "fake"
    bundle = "tmp"
    bundle_link = build_link_to_bundle(cluster, bundle)
    assert bundle_link == "https://yt.yandex-team.ru/{}/tablet_cell_bundles/tablet_cells?activeBundle={}".format(cluster, bundle)


def test_get_quota_holder_threshold():
    account = "fake-account"
    default_threshold = 85
    custom_thresholds = {
        account: 45
    }
    threshold_time_period_overrides = {
        account: {
            "start_time": "00:00:00",
            "end_time": "10:00:00",
            "threshold": 95,
        }
    }
    assert get_quota_holder_threshold(account, default_threshold, {}, {}) == default_threshold
    assert get_quota_holder_threshold(account, default_threshold, custom_thresholds, {}) == custom_thresholds[account]

    system_quotas._get_now_time = mock.MagicMock(return_value=datetime.time(16, 00))
    assert get_quota_holder_threshold(account, default_threshold, custom_thresholds, threshold_time_period_overrides) == custom_thresholds[account]

    system_quotas._get_now_time = mock.MagicMock(return_value=datetime.time(5, 00))
    assert get_quota_holder_threshold(account, default_threshold, custom_thresholds, threshold_time_period_overrides) == threshold_time_period_overrides[account]["threshold"]
