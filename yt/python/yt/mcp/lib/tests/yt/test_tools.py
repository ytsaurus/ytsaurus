import yt.wrapper as yt

from yt.mcp.lib.tools.list_dir import ListDir

from .conftest import authors

from dataclasses import dataclass

import pytest
import re
import logging

logger = logging.getLogger(__name__)


@dataclass
class MCPRequestContextStub:
    auth_token: str


def _sanitize_date(text):
    return re.sub(r"\b\d{4}-\d{2}-\d{2}T.*?Z", "<datetime>", text)


@pytest.mark.usefixtures("yt_env_v4")
@authors("denvr")
def test_list_dir():
    client = yt.YtClient(config=yt.config)
    client.create("map_node", "//tmp/test_list_dir")
    client.create("table", "//tmp/test_list_dir/some_table_1")
    client.create("table", "//tmp/test_list_dir/some_table_2")

    class FakeRunner:
        _logger = logger

        def helper_get_yt_client(self, cluster, request_context):
            return client

        def return_structured(self, data):
            return data

    runner = FakeRunner()
    tool = ListDir()
    tool.set_runner(runner)

    result = tool.on_handle_request(cluster=client.config["proxy"]["url"], directory="//tmp/test_list_dir", request_context=None)

    assert _sanitize_date(str(result)) == "[{'value': 'some_table_1', 'attributes': {'row_count': 0, 'resource_usage': {'node_count': 1, 'chunk_count': 0, 'disk_space_per_medium': {}, 'disk_space': 0, 'chunk_host_cell_master_memory': 0, 'master_memory': 0, 'detailed_master_memory': {'nodes': 0, 'chunks': 0, 'attributes': 0, 'tablets': 0, 'schemas': 0}, 'tablet_count': 0, 'tablet_static_memory': 0}, 'primary_medium': 'default', 'type': 'table', 'account': 'tmp', 'creation_time': '<datetime>'}}, {'value': 'some_table_2', 'attributes': {'row_count': 0, 'resource_usage': {'node_count': 1, 'chunk_count': 0, 'disk_space_per_medium': {}, 'disk_space': 0, 'chunk_host_cell_master_memory': 0, 'master_memory': 0, 'detailed_master_memory': {'nodes': 0, 'chunks': 0, 'attributes': 0, 'tablets': 0, 'schemas': 0}, 'tablet_count': 0, 'tablet_static_memory': 0}, 'primary_medium': 'default', 'type': 'table', 'account': 'tmp', 'creation_time': '<datetime>'}}]"  # noqa
