from .conftest import (
    Cli,
    create_nodes,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_assigned_pod_scheduling_status,
)

from yp.common import wait

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
        self.set_env_patch(dict(YP_ADDRESS=grpc_address, YP_PROTOCOL="grpc"))
        self.set_config(dict(enable_ssl=False))

    def set_config(self, config):
        self._config = yson.dumps(config, yson_format="text")

    def get_args(self, args):
        return super(YpCli, self).get_args(args) + ["--config", self._config]

    def check_yson_output(self, *args, **kwargs):
        result = self.check_output(*args, **kwargs)
        return yson._loads_from_native_str(result)

def create_cli(yp_env):
    return YpCli(yp_env.yp_instance.yp_client_grpc_address)


def create_pod_set(cli):
    return cli.check_output(["create", "pod_set"])

def create_pod(cli, pod_set_id=None):
    if pod_set_id is None:
        pod_set_id = create_pod_set(cli)

    attributes = {"meta": {"pod_set_id": pod_set_id}}
    return cli.check_output([
        "create",
        "pod",
        "--attributes", yson.dumps(attributes)
    ])

def create_user(cli):
    return cli.check_output(["create", "user"])


@pytest.mark.usefixtures("yp_env")
class TestCli(object):
    def test_common(self, yp_env):
        cli = create_cli(yp_env)

        pod_id = create_pod(cli)

        result = cli.check_yson_output([
            "get",
            "pod", pod_id,
            "--selector", "/status/agent/state",
            "--selector", "/meta/id"
        ])
        assert result == ["unknown", pod_id]

        result = cli.check_yson_output([
            "select",
            "pod",
            "--filter", '[/meta/id] = "{}"'.format(pod_id),
            "--no-tabular"
        ])
        assert result == [[]]

    def test_check_object_permission(self, yp_env):
        cli = create_cli(yp_env)

        pod_id = create_pod(cli)

        result = cli.check_yson_output([
            "check-object-permission",
            "pod", pod_id,
            "everyone",
            "read"
        ])
        assert result == dict(action="deny")

        result = cli.check_yson_output([
            "check-permission",
            "pod", pod_id,
            "root",
            "write"
        ])
        assert result == dict(action="allow")

        user_id = create_user(cli)
        yp_env.sync_access_control()

        result = cli.check_yson_output([
            "check-permission",
            "pod", pod_id,
            user_id,
            "read"
        ])
        assert result["action"] == "allow"

    def test_get_object_access_allowed_for(self, yp_env):
        cli = create_cli(yp_env)

        pod_id = create_pod(cli)

        all_user_ids = ["root"]
        for _ in xrange(10):
            all_user_ids.append(create_user(cli))

        result = cli.check_yson_output([
            "get-object-access-allowed-for",
            "pod", pod_id,
            "read"
        ])

        assert "user_ids" in result
        result["user_ids"].sort()

        assert result == dict(user_ids=sorted(all_user_ids))

    def test_get_user_access_allowed_to(self, yp_env):
        cli = create_cli(yp_env)

        yp_env.sync_access_control()

        command = [
            "get-user-access-allowed-to",
            "root",
            "account",
            "read",
        ]
        result = cli.check_yson_output(command)
        assert result["object_ids"] == ["tmp"]

        result = cli.check_yson_output(command + ["--limit", "0"])
        assert result["object_ids"] == []

        object_ids = []
        continuation_token = None
        for _ in range(2):
            extended_command = command + ["--limit", "1"]
            if continuation_token is not None:
                extended_command += ["--continuation-token", continuation_token]
            result = cli.check_yson_output(extended_command)
            object_ids.extend(result["object_ids"])
            continuation_token = result["continuation_token"]

        assert object_ids == ["tmp"]

    def test_binary_data(self, yp_env):
        cli = create_cli(yp_env)

        pod_set_id = cli.check_output([
            "create", "pod_set",
            "--attributes", yson.dumps({"annotations": {"hello": "\x01\x02"}})
        ])

        result = cli.check_yson_output([
            "get",
            "pod_set", pod_set_id,
            "--selector", "/annotations",
        ])
        assert result == [{"hello": "\x01\x02"}]

        result = cli.check_output([
            "get",
            "pod_set", pod_set_id,
            "--selector", "/annotations",
            "--format", "json",
        ])
        assert json.loads(result) == [{"hello": "\x01\x02"}]

    def test_update_hfsm(self, yp_env):
        cli = create_cli(yp_env)

        node_id = yp_env.yp_client.create_object("node")
        cli.check_output(["update-hfsm-state", node_id, "up", "test"])

        result = cli.check_yson_output([
            "get",
            "node", node_id,
            "--selector", "/status/hfsm/state"
        ])

        assert result[0] == "up"

    def test_pod_eviction(self, yp_env):
        cli = create_cli(yp_env)
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)[0]
        pod_set_id = yp_client.create_object("pod_set")
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))
        wait(lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id)))

        get_eviction_state = lambda: cli.check_yson_output([
            "get",
            "pod", pod_id,
            "--selector", "/status/eviction/state"
        ])[0]
        get_eviction_message = lambda: cli.check_yson_output([
            "get",
            "pod", pod_id,
            "--selector", "/status/eviction/message"
        ])[0]

        assert get_eviction_state() == "none"

        message = "hello, eviction!"
        cli.check_output(["request-eviction", pod_id, message])
        assert get_eviction_state() == "requested"
        assert get_eviction_message() == message

        cli.check_output(["abort-eviction", pod_id, "test"])
        assert get_eviction_state() == "none"

        cli.check_output(["request-eviction", pod_id, "test"])
        assert get_eviction_state() == "requested"

        tx_id = yp_client.start_transaction()
        cli.check_output([
            "acknowledge-eviction",
            pod_id, "test",
            "--transaction_id", tx_id
        ])
        assert get_eviction_state() == "requested"
        yp_client.commit_transaction(tx_id)
        assert get_eviction_state() in ("acknowledged", "none")

    def test_touch_pod_master_spec_timestamps(self, yp_env):
        cli = create_cli(yp_env)
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        node_ids = create_nodes(yp_client, 10)
        pod_ids = [create_pod_with_boilerplate(yp_client, pod_set_id, {"node_id": node_id}) for node_id in node_ids]

        def get_timestamps():
            return [t[0] for t in yp_client.get_objects("pod", pod_ids, selectors=["/status/master_spec_timestamp"])]

        timestamps1 = get_timestamps()

        def run_script(pod_ids, tx_id=None):
            cli.check_output(["touch-pod-master-spec-timestamps"]
                             + pod_ids
                             + (["--transaction-id", tx_id] if tx_id else []))

        run_script([])
        timestamps2 = get_timestamps()
        assert timestamps2 == timestamps1
        timestamps1 = timestamps2

        run_script(pod_ids[0:1])
        timestamps2 = get_timestamps()
        assert timestamps2[0] > timestamps1[0] and timestamps1[1:] == timestamps2[1:]
        timestamps1 = timestamps2

        tx_id = yp_client.start_transaction()
        run_script(pod_ids[0:5], tx_id)
        timestamps2 = get_timestamps()
        assert timestamps2 == timestamps1
        yp_client.commit_transaction(tx_id)
        timestamps2 = get_timestamps()
        assert all(timestamps2[i] > timestamps1[i] for i in range(5)) and timestamps1[5:] == timestamps2[5:]
        timestamps1 = timestamps2

    def test_aggregate(self, yp_env):
        cli = create_cli(yp_env)

        pod_set_id = create_pod_set(cli)
        pod_ids = []
        memory_limits = [i * 2**20 for i in range(1, 8)]
        for memory_limit in memory_limits:
            attributes = {
                "meta": {"pod_set_id": pod_set_id},
                "spec": {"resource_requests": {"memory_limit": memory_limit}},
            }
            pod_ids.append(cli.check_output([
                "create",
                "pod",
                "--attributes", yson.dumps(attributes)
            ]))

        result = cli.check_yson_output([
            "aggregate",
            "pod",
            "--group-by", "is_prefix([/meta/pod_set_id], \"{}\")".format(pod_set_id),
            "--group-by", "int64([/status/agent_spec_timestamp]) + 5",
            "--aggregator", "avg(int64([/spec/resource_requests/memory_limit]))",
            "--aggregator", "max([/meta/creation_time])",
            "--filter", "[/meta/pod_set_id] = \"{}\"".format(pod_set_id),
            "--no-tabular",
        ])

        assert len(result) == 1
        assert len(result[0]) == 4
        assert result[0][2] == float(sum(memory_limits)) / len(memory_limits)
