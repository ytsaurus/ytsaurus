from conftest import TESTS_LOCATION

import yt.yson as yson
import yt.subprocess_wrapper as subprocess
from yt.common import update

import pytest

import os
import sys
from copy import deepcopy

@pytest.mark.usefixtures("yp_env")
class TestCli(object):
    def test_common(self, yp_env):
        cli_path = os.path.normpath(os.path.join(TESTS_LOCATION, "../python/yp/bin/yp"))
        env = update(deepcopy(os.environ), {"YP_ADDRESS": yp_env.yp_instance.yp_client_grpc_address})
        config_arg = ["--config", yson.dumps({"enable_ssl": False}, yson_format="text")]

        def check_output(*args, **kwargs):
            return subprocess.check_output(*args, env=env, stderr=sys.stderr, **kwargs)

        pod_set_id = check_output([cli_path, "create", "pod_set"] + config_arg).strip()
        pod_id = check_output([cli_path, "create", "pod", "--attributes", yson.dumps({"meta": {"pod_set_id": pod_set_id}})] + config_arg).strip()

        result = check_output([cli_path, "get", "pod", pod_id, "--selector", "/status/agent/state", "--selector", "/meta/id"] + config_arg)
        assert yson.loads(result) == ["unknown", pod_id]

        result = check_output([cli_path, "select", "pod", "--filter", '[/meta/id] = "{}"'.format(pod_id), "--no-tabular"] + config_arg)
        assert yson.loads(result) == [[]]

