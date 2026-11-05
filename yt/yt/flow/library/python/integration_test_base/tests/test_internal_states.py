import builtins
import importlib
from unittest.mock import Mock

import pytest

from yt.yt.flow.library.python.integration_test_base import yt_flow_base


@pytest.mark.authors(["mikari"])
def test_import_without_state_unpack_bindings(monkeypatch):
    original_import = builtins.__import__

    def import_module(name, *args, **kwargs):
        if name.startswith("yt.yt.flow.library.python.client.state_unpack"):
            raise ImportError("Native state unpack bindings are unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_module)
    importlib.reload(yt_flow_base)


@pytest.mark.authors(["mikari"])
def test_read_internal_key_states():
    base = yt_flow_base.FlowTestBase()
    base.pipeline_path = "//pipeline"
    base.client = Mock()
    base.client.read_states.return_value = {
        "key_states": [
            {"computation_id": "mapper", "key": [1, "a"], "states": {"/first": {"count": 3}, "/second": 7}},
            {"computation_id": "mapper", "key": [2, "b"], "states": {"/first": {"count": 5}}},
        ],
        "partition_states": [],
        "external_key_states": [],
        "joined_external_key_states": [],
        "errors": [],
    }

    assert base.read_internal_key_states("mapper") == [
        {"computation_id": "mapper", "key": [1, "a"], "name": "/first", "state": {"count": 3}},
        {"computation_id": "mapper", "key": [1, "a"], "name": "/second", "state": 7},
        {"computation_id": "mapper", "key": [2, "b"], "name": "/first", "state": {"count": 5}},
    ]
    base.client.read_states.assert_called_once_with(
        "//pipeline", computation_id="mapper", target="key_state", limit=10000
    )
