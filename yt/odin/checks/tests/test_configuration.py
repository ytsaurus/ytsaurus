import yatest.common as yatest_common

import pytest

import json
import imp


def test_configuration():
    config_path = yatest_common.build_path("yt/odin/checks/config/config.json")
    json.load(open(config_path, "r"))


# NB: the test is commented out so that @arivkin can commit from the Arcanum UI :)
'''
def test_configuration_canonical():
    script_path = yatest_common.source_path("yt/odin/checks/config/make_config.py")
    return yatest_common.canonical_py_execute(script_path)
'''


def test_deep_merge():
    path = yatest_common.source_path("yt/odin/checks/config/make_config.py")
    module = imp.load_source("make_config", path)
    deep_merge = module.deep_merge

    assert deep_merge(dict(a="1"), dict(b="2")) == dict(a="1", b="2")
    assert deep_merge(dict(a=dict(foo=5)), dict(a=dict(bar=10))) == dict(a=dict(foo=5, bar=10))

    with pytest.raises(ValueError):
        deep_merge(dict(xyz=dict(abc=123)), dict(xyz=dict(abc=456)))

    with pytest.raises(TypeError):
        deep_merge(5, "abc")
