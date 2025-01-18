from library.python import confmerge


def test_no_settings_no_cry():
    assert {} == confmerge.get_section({}, "k")


def test_merge_copy():
    base = {"a": 1}
    result = confmerge.merge_copy(base, {"b": 2})
    assert result is not base
    assert {"a": 1} == base
    assert {"a": 1, "b": 2} == result


def test_merge_do_not_poison_patch():
    base = {"a": 1}
    patch = {"b": {"c": 2}, "c": [{"a": 1}]}
    confmerge.merge_inplace(base, patch)
    assert base["b"] is not patch["b"]
    assert base["c"] is not patch["c"]
    confmerge.merge_inplace(base, {"b": {"d": 3}})
    assert "d" not in patch["b"]
    assert {"a": 1, "b": {"c": 2, "d": 3}, 'c': [{'a': 1}]} == base


def test_merge_inplace():
    base = {"a": 1}
    result = confmerge.merge_inplace(base, {"b": 2})
    assert result is base
    assert {"a": 1, "b": 2} == base


def test_recursive_inheritance():
    config = {
        "default": {"a": 1},
        "key1": {
            "a": 2,
            "b": 1,
            "c": 4,
        },
        "key2": {
            "inherit": "key1",
            "a": 3,
            "b": 2,
        },
        "key3": {
            "inherit": "key2",
            "a": 5,
        },
    }

    original = repr(config)
    s = confmerge.get_section(config, "key3")
    assert original == repr(config), "settings should not be modified!"
    assert {"a": 5, "b": 2, "c": 4} == s


def test_reverse_get_section():
    config = {
        "default": {
            "key1": {
                "key11": "value11",
            },
            "key2": {
                "key21": ["value211", "value212"],
            },
        },
        "a": {
            "key1": {},
            "key2": {},
        },
        "b": {
            "inherit": "a",
            "key1": {
                "key11": "value11b",
            },
            "key2": {
                "key21": ["value211b"],
            },
        },
    }

    b_conf = confmerge.get_section(config, "b")

    assert b_conf["key1"]["key11"] == "value11b"
    assert b_conf["key2"]["key21"] == ["value211b"]

    a_conf = confmerge.get_section(config, "a")

    assert a_conf["key1"]["key11"] == "value11"
    assert a_conf["key2"]["key21"] == ["value211", "value212"]


def test_get_section():
    config = {
        "a": {"key1": 1},
        "b": {"inherit": "a", "key2": 0},
        "default": {"key2": 2},
    }
    original = repr(config)
    a = confmerge.get_section(config, "a")
    assert original == repr(config)
    assert {"key1": 1, "key2": 2} == a
    b = confmerge.get_section(config, "b")
    assert original == repr(config)
    assert {"key1": 1, "key2": 0} == b


def test_apply_inherit_recursively():
    config = {
        "a": {"key1": 1},
        "b": {"a": {"key1": 10, "key2": 20}, "b": {"inherit": "a", "key1": 15}},
        "c": {"inherit": "b", "a": {"key1": 25}},
    }
    confmerge.apply_inherit_recursively(config)
    expected = {
        "a": {"key1": 1},
        "b": {"a": {"key1": 10, "key2": 20}, "b": {"key1": 15, "key2": 20}},
        "c": {"a": {"key1": 25, "key2": 20}, "b": {"key1": 15, "key2": 20}},
    }
    assert expected == config
