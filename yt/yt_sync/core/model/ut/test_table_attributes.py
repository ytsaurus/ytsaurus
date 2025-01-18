import pytest

from yt.yt_sync.core.model.table_attributes import YtTableAttributes


class TestYtTableAttributes:
    @pytest.mark.parametrize(
        "untracked_attr", ["upstream_replica_id", "max_unix_time", "yabs_data_time", "last_sync_time1"]
    )
    def test_make_with_untracked(self, untracked_attr: str):
        yt_attrs = YtTableAttributes.make({"attr1": "v1", untracked_attr: "1"})
        assert "attr1" in yt_attrs.attributes
        assert untracked_attr not in yt_attrs.attributes

    @pytest.mark.parametrize(
        "untracked_attr", ["upstream_replica_id", "max_unix_time", "yabs_data_time", "last_sync_time1"]
    )
    def test_yt_attributes_with_untracked(self, untracked_attr: str):
        yt_attrs = YtTableAttributes.make({"attr1": "v1"})
        yt_attrs[untracked_attr] = "1"
        assert untracked_attr in yt_attrs.attributes
        assert {"attr1": "v1"} == yt_attrs.yt_attributes

    @pytest.mark.parametrize(
        "untracked_attr", ["upstream_replica_id", "max_unix_time", "yabs_data_time", "last_sync_time1"]
    )
    def test_has_diff_with_untracked(self, untracked_attr: str):
        attrs1 = YtTableAttributes.make({"attr1": "v1"})
        attrs1[untracked_attr] = "1"
        attrs2 = YtTableAttributes.make({"attr1": "v1"})
        attrs2[untracked_attr] = "2"
        assert not attrs1.has_diff_with(attrs2)

    def test_missing_user_attributes(self):
        yt_attrs = YtTableAttributes.make({"attr1": "v1", "attr2": "v1", "user_attribute_keys": ["attr2", "attr3"]})
        assert set(["attr3"]) == yt_attrs.missing_user_attributes

    @pytest.mark.parametrize(
        "untracked_attr", ["upstream_replica_id", "max_unix_time", "yabs_data_time", "last_sync_time1"]
    )
    def test_missing_user_attributes_with_untracked(self, untracked_attr: str):
        yt_attrs = YtTableAttributes.make({"attr1": "v1", "user_attribute_keys": ["attr2", untracked_attr]})
        assert set(["attr2"]) == yt_attrs.missing_user_attributes

    def test_actual_only_attributes_are_not_diff(self):
        actual_attrs = YtTableAttributes.make({"key": "value", "data_weight": 1024 * 1024 * 100})
        desired_attrs = YtTableAttributes.make({"key": "value"})
        diff = list(actual_attrs.changed_attributes(desired_attrs))
        assert diff == []

    def test_filter_does_not_apply_to_actual_only(self):
        desired_attrs = YtTableAttributes.make({"key": "value"})
        actual_attrs = YtTableAttributes.make({"key": "value", "data_weight": 1024 * 1024 * 100}, desired_attrs)
        assert actual_attrs["data_weight"]

    @pytest.mark.parametrize(
        "untracked_attr", ["upstream_replica_id", "max_unix_time", "yabs_data_time", "last_sync_time1"]
    )
    def test_changed_attributes_with_untracked(self, untracked_attr: str):
        attrs1 = YtTableAttributes.make({"attr1": "v1"})
        attrs1[untracked_attr] = "1"
        attrs2 = YtTableAttributes.make({"attr1": "v1"})
        attrs2[untracked_attr] = "2"

        diff = dict()
        for path, desired, actual in attrs1.changed_attributes(attrs2):
            diff[path] = (desired, actual)

        assert {} == diff

    def test_is_unmount_required_not(self):
        yt_attrs1 = YtTableAttributes.make({"k1": "v1", "k2": "v2"})
        yt_attrs2 = YtTableAttributes.make({"k1": "v11", "k2": "v22"})
        assert not yt_attrs1.is_unmount_required(yt_attrs2)

    def test_is_unmount_required(self):
        yt_attrs1 = YtTableAttributes.make({"k1": "v1", "k2": "v2"})
        yt_attrs2 = YtTableAttributes.make({"k1": "v11", "k2": "v22", "primary_medium": "ssd"})
        assert yt_attrs2.is_unmount_required(yt_attrs1)
        assert not yt_attrs2.is_unmount_required(yt_attrs2)

    def test_is_unmount_required_for_hunk_primary_medium(self):
        yt_attrs1 = YtTableAttributes.make({"k1": "v1", "k2": "v2"})
        yt_attrs2 = YtTableAttributes.make({"k1": "v11", "k2": "v22", "hunk_primary_medium": "ssd"})
        assert yt_attrs2.is_unmount_required(yt_attrs1)
        assert not yt_attrs2.is_unmount_required(yt_attrs2)

    def test_is_remount_required(self):
        yt_attrs1 = YtTableAttributes.make({"k1": "v1", "k2": "v2"})
        yt_attrs2 = YtTableAttributes.make({"k1": "v11", "k2": "v22"})
        assert yt_attrs1.is_remount_required(yt_attrs2)

    def test_is_remount_required_not(self):
        yt_attrs1 = YtTableAttributes.make({"version": 1})
        yt_attrs2 = YtTableAttributes.make({"version": 2})
        assert not yt_attrs1.is_remount_required(yt_attrs2)

    @pytest.mark.parametrize("regular_attr_pattern", ["static_export_config"])
    def test_regular_attribute_pattern_remount_not_required(self, regular_attr_pattern: str):
        yt_attrs1 = YtTableAttributes.make({regular_attr_pattern: {"deep_key": "123"}})
        yt_attrs2 = YtTableAttributes.make({regular_attr_pattern: {"deep_key": "456"}})
        assert not yt_attrs1.is_remount_required(yt_attrs2)
