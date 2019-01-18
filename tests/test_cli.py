from .conftest import Cli

import yt.yson as yson

from yt.packages.six.moves import xrange

try:
    import yt.json_wrapper as json
except ImportError:
    import yt.json as json

import pytest


class YpCli(Cli):
    def __init__(self, grpc_address):
        super(YpCli, self).__init__("python/yp/bin", "yp_make", "yp")
        self.set_env_patch(dict(YP_ADDRESS=grpc_address))
        self.set_config(dict(enable_ssl=False))

    def set_config(self, config):
        self._config = yson.dumps(config, yson_format="text")

    def get_args(self, args):
        return super(YpCli, self).get_args(args) + ["--config", self._config]

def create_cli(yp_env):
    return YpCli(yp_env.yp_instance.yp_client_grpc_address)


def create_pod(cli):
    pod_set_id = cli.check_output(["create", "pod_set"])
    return cli.check_output([
        "create",
        "pod",
        "--attributes", yson.dumps({"meta": {"pod_set_id": pod_set_id}})
    ])

def create_user(cli):
    return cli.check_output(["create", "user"])


@pytest.mark.usefixtures("yp_env")
class TestCli(object):
    def test_common(self, yp_env):
        cli = create_cli(yp_env)

        pod_id = create_pod(cli)

        result = cli.check_output([
            "get",
            "pod", pod_id,
            "--selector", "/status/agent/state",
            "--selector", "/meta/id"
        ])
        assert yson._loads_from_native_str(result) == ["unknown", pod_id]

        result = cli.check_output([
            "select",
            "pod",
            "--filter", '[/meta/id] = "{}"'.format(pod_id),
            "--no-tabular"
        ])
        assert yson._loads_from_native_str(result) == [[]]

    def test_check_object_permission(self, yp_env):
        cli = create_cli(yp_env)

        pod_id = create_pod(cli)

        result = cli.check_output([
            "check-object-permission",
            "pod", pod_id,
            "everyone",
            "read"
        ])
        assert yson._loads_from_native_str(result) == dict(action="deny")

        result = cli.check_output([
            "check-permission",
            "pod", pod_id,
            "root",
            "write"
        ])
        assert yson._loads_from_native_str(result) == dict(action="allow")

        user_id = create_user(cli)
        yp_env.sync_access_control()

        result = cli.check_output([
            "check-permission",
            "pod", pod_id,
            user_id,
            "read"
        ])
        assert yson._loads_from_native_str(result) == dict(action="allow", object_type="schema", object_id="pod_set", subject_id="everyone")

    def test_get_object_access_allowed_for(self, yp_env):
        cli = create_cli(yp_env)

        pod_id = create_pod(cli)

        all_user_ids = ["root"]
        for _ in xrange(10):
            all_user_ids.append(create_user(cli))

        result_str = cli.check_output([
            "get-object-access-allowed-for",
            "pod", pod_id,
            "read"
        ])
        result = yson._loads_from_native_str(result_str)

        assert "user_ids" in result
        result["user_ids"].sort()

        assert result == dict(user_ids=sorted(all_user_ids))

    def test_binary_data(self, yp_env):
        cli = create_cli(yp_env)

        pod_set_id = cli.check_output([
            "create", "pod_set",
            "--attributes", yson.dumps({"annotations": {"hello": "\x01\x02"}})
        ])

        result = cli.check_output([
            "get",
            "pod_set", pod_set_id,
            "--selector", "/annotations",
        ])
        assert yson._loads_from_native_str(result) == [{"hello": "\x01\x02"}]

        result = cli.check_output([
            "get",
            "pod_set", pod_set_id,
            "--selector", "/annotations",
            "--format", "json",
        ])
        assert json.loads(result) == [{"hello": "\x01\x02"}]

