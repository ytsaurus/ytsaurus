from yt_mock_server import MockServer

import pytest

pytest_plugins = [
    "yt.test_helpers.authors",
    "yt.test_helpers.set_timeouts",
    "yt.test_helpers.filter_by_category",
    "yt.test_helpers.fork_class"
]


@pytest.fixture
def mock_server(request):
    server = MockServer(port=request.node.cls.mock_server_port)
    try:
        server.up()
        yield server
    finally:
        server.down()
