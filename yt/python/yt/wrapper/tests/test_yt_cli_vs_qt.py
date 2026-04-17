from .conftest import authors
from .helpers import get_tests_sandbox, get_environment_for_binary_test
from .helpers_cli import YtCli

from yt.common import makedirp

import os
import pytest
import uuid


@pytest.fixture()
def yt_cli_vs_qt(request: pytest.FixtureRequest, yt_env_v4):
    env = get_environment_for_binary_test(yt_env_v4)

    sandbox_root: str = get_tests_sandbox()  # type: ignore

    test_name = request.node.name
    sandbox_dir = os.path.join(sandbox_root, f"TestYtBinaryVsQt_{test_name}_" + uuid.uuid4().hex[:8])
    makedirp(sandbox_dir)
    replace = {
        "yt": [env["PYTHON_BINARY"], env["YT_CLI_PATH"]]
    }
    yield YtCli(env, sandbox_dir, replace)


@pytest.mark.usefixtures("yt_env_v4")
class TestYtBinaryVsQt(object):
    @authors("a-romanov")
    def test_no_result(self, yt_cli_vs_qt: YtCli, yt_query_tracker_v4):
        assert yt_cli_vs_qt.check_output(["yt", "query", "mock", "--format", "json", "complete_after"]) == b""

    @authors("a-romanov")
    def test_single_result(self, yt_cli_vs_qt: YtCli, yt_query_tracker_v4):
        r = yt_cli_vs_qt.check_output(["yt", "query", "mock", "--format", "json", "complete_after", "--settings", "{results=[{schema=[{name=baz;type=boolean}];rows=[{baz=%true}]}]}"])
        assert r == b'{"baz":true}\n'
