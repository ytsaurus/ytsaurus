import yt.logger as yt_logger
import yt.yson as yson

from yt.wrapper.config_remote_patch import RemotePatchableValueBase
from yt.wrapper.errors import YtResolveError

from unittest import mock

from yt.testlib import authors


@authors("papilov")
def test_config_remote_patch_misconfiguration_warnings(monkeypatch):
    monkeypatch.delenv("YT_APPLY_REMOTE_PATCH_AT_START", raising=False)

    def _document(value):
        node = yson.YsonEntity()
        node.attributes["type"] = "document"
        node.attributes["value"] = yson.YsonMap(value)
        return node

    def _map_node(children):
        node = yson.YsonMap(children)
        node.attributes["type"] = "map_node"
        return node

    def _fetch_remote_patch(data):
        client = mock.Mock()
        client.config = {
            "apply_remote_patch_at_start": True,
            "config_remote_patch_path": "//sys/client_config",
            "proxy": {"url": "test_cluster"},
        }
        if isinstance(data, Exception):
            client.get.side_effect = data
        else:
            client.get.return_value = data
        with mock.patch.object(RemotePatchableValueBase, "_REMOTE_CACHE", {}), \
                mock.patch.object(yt_logger.LOGGER, "warning") as warning_mock, \
                mock.patch.object(yt_logger.LOGGER, "error"):
            patch = RemotePatchableValueBase._get_remote_patch(client)
        assert client.get.call_count == 1, "misconfiguration checks should not add requests"
        assert client.get.call_args.kwargs["attributes"] == ["value", "type"]
        warnings = [call.args[0] % tuple(call.args[1:]) for call in warning_mock.call_args_list]
        return patch, warnings

    # Healthy configuration: no warnings, patch is applied.
    healthy_data = _map_node({"default": _document({"remote_option": 1})})
    patch, warnings = _fetch_remote_patch(healthy_data)
    assert warnings == []
    assert patch == {"remote_option": 1}

    # //sys/client_config exists, but is not a map_node (scalar node).
    string_node = yson.YsonString(b"garbage")
    string_node.attributes["type"] = "string_node"
    patch, warnings = _fetch_remote_patch(string_node)
    assert not patch
    assert len(warnings) == 1
    assert "\"//sys/client_config\" exists, but has type \"string_node\" instead of \"map_node\"" \
        in warnings[0]

    # //sys/client_config exists, but is a document (its content is returned by get).
    document_content = _map_node({"default": yson.YsonMap({"remote_option": 1})})
    document_content.attributes["type"] = "document"
    patch, warnings = _fetch_remote_patch(document_content)
    assert not patch
    assert len(warnings) == 1
    assert "has type \"document\" instead of \"map_node\"" in warnings[0]

    # //sys/client_config/default exists, but is not a document.
    patch, warnings = _fetch_remote_patch(_map_node({"default": _map_node({"remote_option": 1})}))
    assert not patch
    assert len(warnings) == 1
    assert "\"//sys/client_config/default\" exists, but has type \"map_node\" " \
        "instead of \"document\"" in warnings[0]

    # //sys/client_config exists, but the required config //sys/client_config/default is absent.
    patch, warnings = _fetch_remote_patch(_map_node({}))
    assert not patch
    assert len(warnings) == 1
    assert "required config \"//sys/client_config/default\" is absent" in warnings[0]

    # No warnings when the cluster does not have //sys/client_config at all.
    patch, warnings = _fetch_remote_patch(YtResolveError({"code": 500, "message": "resolve error"}))
    assert not patch
    assert warnings == []
