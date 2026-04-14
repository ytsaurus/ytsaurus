import yt.wrapper as yt

from yt.mcp.lib.tools.list_dir import ListDir, Search

from .conftest import authors

import pytest
import re
import typing
import logging

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("yt_env_v4")
class TestSimpleTool:
    def _mask_date(self, data):
        if isinstance(data, yt.yson.YsonUnicode):
            if data.attributes:
                return {"value": self._mask_date(str(data)), "attributes": self._mask_date(data.attributes)}
            else:
                return self._mask_date(str(data))
        elif isinstance(data, (str, bytes)):
            return re.sub(r"\b\d{4}-\d{2}-\d{2}T.*?Z", "<datetime>", data)
        elif isinstance(data, typing.Mapping):
            return dict((k, self._mask_date(v)) for k, v in data.items())
        elif isinstance(data, typing.Iterable):
            return [self._mask_date(i) for i in data]
        else:
            return data

    def _request(self, client, tool, *args, **kwargs):
        class FakeRunner:
            _logger = logger

            def helper_get_yt_client(self, cluster, request_context):
                return client

            def return_structured(self, data):
                return data

        runner = FakeRunner()
        tool.set_runner(runner)

        result = tool.on_handle_request(*args, request_context=None, **kwargs)
        if isinstance(result, typing.Generator):
            result = list(result)
        return self._mask_date(result)

    @authors("denvr")
    def test_list_dir(self):
        client = yt.YtClient(config=yt.config)
        client.create("map_node", "//tmp/test_list_dir")
        client.create("table", "//tmp/test_list_dir/some_table_1")
        client.create("table", "//tmp/test_list_dir/some_table_2")

        result = self._request(client, ListDir(), cluster=client.config["proxy"]["url"], directory="//tmp/test_list_dir")

        assert result == [
            {"value": "some_table_1", "attributes": {"row_count": 0, "resource_usage": {"node_count": 1, "chunk_count": 0, "disk_space_per_medium": {}, "disk_space": 0, "chunk_host_cell_master_memory": 0, "master_memory": 0, "detailed_master_memory": {"nodes": 0, "chunks": 0, "attributes": 0, "tablets": 0, "schemas": 0}, "tablet_count": 0, "tablet_static_memory": 0}, "primary_medium": "default", "type": "table", "account": "tmp", "creation_time": "<datetime>"}},  # noqa
            {"value": "some_table_2", "attributes": {"row_count": 0, "resource_usage": {"node_count": 1, "chunk_count": 0, "disk_space_per_medium": {}, "disk_space": 0, "chunk_host_cell_master_memory": 0, "master_memory": 0, "detailed_master_memory": {"nodes": 0, "chunks": 0, "attributes": 0, "tablets": 0, "schemas": 0}, "tablet_count": 0, "tablet_static_memory": 0}, "primary_medium": "default", "type": "table", "account": "tmp", "creation_time": "<datetime>"}},  # noqa
        ]

    @authors("denvr")
    def test_search(self):
        client = yt.YtClient(config=yt.config)
        client.create("map_node", "//tmp/test_list_dir")
        client.create("table", "//tmp/test_list_dir/some_table_1")
        client.create("table", "//tmp/test_list_dir/some_table_2")

        result = self._request(client, Search(), cluster=client.config["proxy"]["url"], root_path="//tmp", name=None, type="table", attributes_to_match=None, attributes=None)

        assert result == [
            {"value": "//tmp/test_list_dir/some_table_1", "attributes": {"type": "table", "account": "tmp", "creation_time": "<datetime>"}},
            {"value": "//tmp/test_list_dir/some_table_2", "attributes": {"type": "table", "account": "tmp", "creation_time": "<datetime>"}},
        ]
