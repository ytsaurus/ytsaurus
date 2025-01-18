import pytest

from yt.yson.yson_types import YsonEntity
from yt.yt_sync.core.model import YtNodeAttributes


class TestYtNodeAttributes:
    def test_make(self):
        attrs = {"attr1": "v1", "attr2": YsonEntity()}
        yt_attrs = YtNodeAttributes.make(attrs)
        assert yt_attrs.attributes["attr1"] == "v1"
        assert yt_attrs.attributes["attr2"] is None
        assert not yt_attrs.user_attribute_keys

    def test_make_with_filter(self):
        attrs1 = {"k1": "v1", "k2": "v2", "k4": {"k41": 1, "k42": 2}}
        yt_attrs1 = YtNodeAttributes.make(attrs1)

        attrs2 = {"k2": "v2", "k3": "v3", "k4": {"k41": 10}}
        yt_attrs2 = YtNodeAttributes.make(attrs2, yt_attrs1)

        assert 2 == len(yt_attrs2.attributes)
        assert "k2" in yt_attrs2.attributes
        assert "k4" in yt_attrs2.attributes
        k4_attrs = yt_attrs2.attributes["k4"]
        assert 1 == len(k4_attrs)
        assert "k41" in k4_attrs

    def test_user_attribute_keys(self):
        attrs = {"attr1": "v1", "attr2": "v2", "user_attribute_keys": ["attr2"]}
        yt_attrs = YtNodeAttributes.make(attrs)
        for key in ("attr1", "attr2"):
            assert key in yt_attrs.attributes
        assert "user_attribute_keys" not in yt_attrs.attributes
        assert 1 == len(yt_attrs.user_attribute_keys)
        assert "attr2" in yt_attrs.user_attribute_keys

    def test_dict_properties(self):
        attrs = {"attr1": "v1", "attr2": "v2"}
        yt_attrs = YtNodeAttributes.make(attrs)
        yt_attrs["attr3"] = "v3"
        assert "attr1" in yt_attrs
        assert "v1" == yt_attrs["attr1"]
        assert "v1" == yt_attrs.get("attr1")
        keys = set([key for key in yt_attrs])
        assert set(["attr1", "attr2", "attr3"]) == keys
        assert "v2" == yt_attrs.pop("attr2", None)
        assert "attr2" not in yt_attrs
        assert "v3" == yt_attrs.get("attr3")

    def test_get_filtered(self):
        assert YtNodeAttributes.make({"a": YsonEntity()}).get_filtered("a") is None
        assert YtNodeAttributes.make({"a": None}).get_filtered("a") is None
        assert 1 == YtNodeAttributes.make({"a": 1}).get_filtered("a")
        assert {"b": 1} == YtNodeAttributes.make({"a": {"b": 1, "c": YsonEntity()}}).get_filtered("a")
        assert {"b": 1} == YtNodeAttributes.make({"a": {"b": 1, "c": None}}).get_filtered("a")

    def test_filtered_value(self):
        assert YtNodeAttributes.filtered_value(YsonEntity()) is None
        assert YtNodeAttributes.filtered_value(None) is None
        assert 1 == YtNodeAttributes.filtered_value(1)
        assert {"b": 1} == YtNodeAttributes.filtered_value({"b": 1, "c": YsonEntity()})
        assert {"b": 1} == YtNodeAttributes.filtered_value({"b": 1, "c": None})

    def test_yt_attrubutes(self):
        attrs = {"attr1": "v1", "attr2": YsonEntity(), "attr3": None, "user_attribute_keys": ["attr2"]}
        yt_attrs = YtNodeAttributes.make(attrs)
        assert {"attr1": "v1"} == yt_attrs.yt_attributes

    def test_has_diff_with_bad_type(self):
        yt_attrs = YtNodeAttributes.make({})
        with pytest.raises(TypeError):
            _ = yt_attrs.has_diff_with(1)

    def test_has_diff_with_same(self):
        attrs1 = YtNodeAttributes.make({"attr1": "v1", "attr2": "v2"})
        attrs2 = YtNodeAttributes.make({"attr1": "v1", "attr2": "v2"})
        assert not attrs1.has_diff_with(attrs2)

    def test_has_diff_with_not_same(self):
        attrs1 = YtNodeAttributes.make({"attr1": "v1", "attr2": "v2"})
        attrs2 = YtNodeAttributes.make({"attr1": "v1", "attr2": "v3"})
        assert attrs1.has_diff_with(attrs2)

    def test_has_diff_with_extra(self):
        attrs1 = YtNodeAttributes.make({"k1": "v1"})
        attrs2 = YtNodeAttributes.make({"k1": "v1", "k2": "v2"})
        assert not attrs1.has_diff_with(attrs2)

    def test_missing_user_attributes(self):
        yt_attrs = YtNodeAttributes.make({"attr1": "v1", "attr2": "v1", "user_attribute_keys": ["attr2", "attr3"]})
        assert set(["attr3"]) == yt_attrs.missing_user_attributes

    def test_changed_attributes(self):
        yt_attrs1 = YtNodeAttributes.make(
            {"k1": "v1", "k2": "v2", "k3": {"k31": "v31", "k32": "v32", "k33": "v33"}, "k4": "v4"}
        )
        yt_attrs2 = YtNodeAttributes.make({"k1": "v1", "k2": "'v2", "k3": {"k31": "v31", "k32": "'v32"}})

        diff = dict()
        for path, desired, actual in yt_attrs1.changed_attributes(yt_attrs2):
            diff[path] = (desired, actual)

        expected_diff = {
            "k2": ("v2", "'v2"),
            "k3/k32": ("v32", "'v32"),
            "k3/k33": ("v33", None),
            "k4": ("v4", None),
        }
        assert expected_diff == diff

    def test_remove_value(self):
        yt_attrs = YtNodeAttributes.make({"k1": "v1", "k2": "v2", "k3": {"k31": "v31", "k32": "v32"}})
        yt_attrs.remove_value("k1")
        assert "k1" not in yt_attrs.attributes
        assert "k2" in yt_attrs.attributes
        yt_attrs.remove_value("k3/k31")
        assert "k3" in yt_attrs.attributes
        assert "k31" not in yt_attrs.attributes["k3"]
        assert "k32" in yt_attrs.attributes["k3"]

    def test_set_value(self):
        yt_attrs = YtNodeAttributes.make({"k1": "v1", "k3": {"k31": "v31"}})
        yt_attrs.set_value("k1", "'v1")
        assert "'v1" == yt_attrs.attributes["k1"]
        yt_attrs.set_value("k2", "v2")
        assert "v2" == yt_attrs.attributes["k2"]
        yt_attrs.set_value("k3/k31", "'v31")
        assert "'v31" == yt_attrs.attributes["k3"]["k31"]
        yt_attrs.set_value("k3/k32", "v32")
        assert "v32" == yt_attrs.attributes["k3"]["k32"]
        yt_attrs.set_value("k4/k41", "v41")
        assert "v41" == yt_attrs.attributes["k4"]["k41"]

    def test_get_value(self):
        yt_attrs = YtNodeAttributes.make({"k1": "v1", "k3": {"k31": "v31"}})
        assert "v1" == yt_attrs.get_value("k1")
        assert yt_attrs.get_value("_unknown_") is None
        assert "unknown" == yt_attrs.get_value("_unknown_", "unknown")
        assert "v31" == yt_attrs.get_value("k3/k31")
        assert yt_attrs.get_value("k3/_unknown_") is None
        assert "unknown" == yt_attrs.get_value("k3/_unknown_", "unknown")

    def test_has_value(self):
        yt_attrs = YtNodeAttributes.make({"k1": "v1", "k3": {"k31": "v31"}})
        assert yt_attrs.has_value("k1")
        assert not yt_attrs.has_value("_unknown_")
        assert yt_attrs.has_value("k3/k31")
        assert not yt_attrs.has_value("k3/_unknown_")
