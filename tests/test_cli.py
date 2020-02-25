from .conftest import (
    Cli,
    create_nodes,
    create_pod_set,
    create_pod_with_boilerplate,
    get_pod_scheduling_status,
    is_assigned_pod_scheduling_status,
    is_pod_assigned,
    wait_pod_is_assigned,
)

from yp.common import wait

import yt.yson as yson

from yt.packages.six.moves import xrange

try:
    import yt.json_wrapper as json
except ImportError:
    import yt.json as json

import pytest


def prepare_objects(yp_client):
    node_id = create_nodes(yp_client, 1)[0]
    pod_set_id = yp_client.create_object("pod_set")
    pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))
    wait_pod_is_assigned(yp_client, pod_id)
    return node_id, pod_set_id, pod_id


class YpCli(Cli):
    def __init__(self, grpc_address):
        super(YpCli, self).__init__("yp/python/yp/bin/yp_make/yp")
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


def create_pod_set_via_cli(cli):
    return cli.check_output(["create", "pod_set"])


def create_pod_via_cli(cli, pod_set_id=None):
    if pod_set_id is None:
        pod_set_id = create_pod_set_via_cli(cli)

    attributes = {"meta": {"pod_set_id": pod_set_id}}
    return cli.check_output(["create", "pod", "--attributes", yson.dumps(attributes)])


def create_user_via_cli(cli):
    return cli.check_output(["create", "user"])


@pytest.mark.usefixtures("yp_env")
class TestCli(object):
    def test_common(self, yp_env):
        cli = create_cli(yp_env)

        pod_id = create_pod_via_cli(cli)

        result = cli.check_yson_output(
            ["get", "pod", pod_id, "--selector", "/status/agent/state", "--selector", "/meta/id"]
        )
        assert result == ["unknown", pod_id]

        result = cli.check_yson_output(
            ["select", "pod", "--filter", '[/meta/id] = "{}"'.format(pod_id), "--no-tabular"]
        )
        assert result == [[]]

    def test_check_object_permission(self, yp_env):
        cli = create_cli(yp_env)

        pod_id = create_pod_via_cli(cli)

        result = cli.check_yson_output(
            ["check-object-permission", "pod", pod_id, "everyone", "read"]
        )
        assert result == dict(action="deny")

        result = cli.check_yson_output(["check-permission", "pod", pod_id, "root", "write"])
        assert result == dict(action="allow")

        user_id = create_user_via_cli(cli)
        yp_env.sync_access_control()

        result = cli.check_yson_output(["check-permission", "pod", pod_id, user_id, "read"])
        assert result["action"] == "allow"

    def test_get_object_access_allowed_for(self, yp_env):
        cli = create_cli(yp_env)

        pod_id = create_pod_via_cli(cli)

        all_user_ids = ["root"]
        for _ in xrange(10):
            all_user_ids.append(create_user_via_cli(cli))

        result = cli.check_yson_output(["get-object-access-allowed-for", "pod", pod_id, "read"])

        assert "user_ids" in result
        result["user_ids"].sort()

        assert result == dict(user_ids=sorted(all_user_ids))

    def test_get_user_access_allowed_to(self, yp_env):
        cli = create_cli(yp_env)

        yp_env.sync_access_control()

        command = ["get-user-access-allowed-to", "root", "account", "read", "--filter", "true"]
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

        def _prepare_pod_set(field_value):
            return ("pod_set", dict(labels=dict(some_field=field_value)))

        pod_set_ids = yp_env.yp_client.create_objects([_prepare_pod_set(1), _prepare_pod_set(2)])
        yp_env.yp_client.create_objects([_prepare_pod_set(3), _prepare_pod_set(4)])

        assert set(
            cli.check_yson_output(
                [
                    "get-user-access-allowed-to",
                    "root",
                    "pod_set",
                    "read",
                    "--filter",
                    "[/labels/some_field]<={}".format(2),
                ]
            )["object_ids"]
        ) == set(pod_set_ids)

    def test_binary_data(self, yp_env):
        cli = create_cli(yp_env)

        pod_set_id = cli.check_output(
            [
                "create",
                "pod_set",
                "--attributes",
                yson.dumps({"annotations": {"hello": "\x01\x02"}}),
            ]
        )

        result = cli.check_yson_output(
            ["get", "pod_set", pod_set_id, "--selector", "/annotations",]
        )
        assert result == [{"hello": "\x01\x02"}]

        result = cli.check_output(
            ["get", "pod_set", pod_set_id, "--selector", "/annotations", "--format", "json",]
        )
        assert json.loads(result) == [{"hello": "\x01\x02"}]

    def test_update_hfsm(self, yp_env):
        cli = create_cli(yp_env)

        node_id = yp_env.yp_client.create_object("node")
        cli.check_output(["update-hfsm-state", node_id, "up", "test"])

        result = cli.check_yson_output(["get", "node", node_id, "--selector", "/status/hfsm/state"])

        assert result[0] == "up"

    def test_touch_pod_master_spec_timestamps(self, yp_env):
        cli = create_cli(yp_env)
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        node_ids = create_nodes(yp_client, 10)
        pod_ids = [
            create_pod_with_boilerplate(yp_client, pod_set_id, {"node_id": node_id})
            for node_id in node_ids
        ]

        def get_timestamps():
            return [
                t[0]
                for t in yp_client.get_objects(
                    "pod", pod_ids, selectors=["/status/master_spec_timestamp"]
                )
            ]

        timestamps1 = get_timestamps()

        def run_script(pod_ids, tx_id=None):
            cli.check_output(
                ["touch-pod-master-spec-timestamps"]
                + pod_ids
                + (["--transaction-id", tx_id] if tx_id else [])
            )

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
        assert (
            all(timestamps2[i] > timestamps1[i] for i in range(5))
            and timestamps1[5:] == timestamps2[5:]
        )

    def test_pod_resources_reallocation(self, yp_env):
        cli = create_cli(yp_env)
        yp_client = yp_env.yp_client

        def _get_the_only_ip6_address_allocation(pod_dump):
            allocations = pod_dump["status"]["ip6_address_allocations"]
            assert len(allocations) == 1
            return allocations[0]

        def _get_the_only_address_by_fqdn(fqdn):
            records = yp_client.get_object("dns_record_set", fqdn, selectors=["/spec/records"],)[0]
            assert len(records) == 1
            return records[0]["data"]

        node_count = 2
        vlan_id = "backbone"

        create_nodes(yp_client, node_count, cpu_total_capacity=200, vlan_id=vlan_id)
        pod_set_id = create_pod_set(yp_client)
        network_project_id = yp_client.create_object(
            "network_project", {"meta": {"id": "somenet"}, "spec": {"project_id": 42},}
        )

        def _create_pod(enable_scheduling):
            return yp_client.create_object(
                "pod",
                {
                    "meta": {"pod_set_id": pod_set_id},
                    "spec": {
                        "enable_scheduling": enable_scheduling,
                        "ip6_address_requests": [
                            {
                                "vlan_id": vlan_id,
                                "network_id": network_project_id,
                                "enable_dns": True,
                                "labels": {"some_key": "some_value"},
                            }
                        ],
                        "resource_requests": {
                            "vcpu_guarantee": 100,
                            "vcpu_limit": 100,
                            "memory_guarantee": 128 * 1024 * 1024,
                            "memory_limit": 128 * 1024 * 1024,
                        },
                    },
                },
            )

        pod_id = _create_pod(True)
        nonschedulable_pod_id = _create_pod(False)

        wait(lambda: is_pod_assigned(yp_client, pod_id))

        initial_pod_dump = yp_client.get_object("pod", pod_id, [""])[0]
        initial_pod_dump_nonschedulable = yp_client.get_object("pod", nonschedulable_pod_id, [""])[
            0
        ]

        initial_allocation = _get_the_only_ip6_address_allocation(initial_pod_dump)
        assert (
            _get_the_only_address_by_fqdn(initial_allocation["persistent_fqdn"])
            == initial_allocation["address"]
        )

        node_id = initial_pod_dump["spec"]["node_id"]
        yp_client.update_object(
            "node", node_id, [{"path": "/spec/ip6_subnets/0/subnet", "value": "4:3:2:1::/64",}]
        )

        cli.check_output(["reallocate-pod-resources", pod_id])
        cli.check_output(["reallocate-pod-resources", nonschedulable_pod_id])

        updated_pod_dump = yp_client.get_object("pod", pod_id, [""])[0]
        updated_pod_dump_nonschedulable = yp_client.get_object("pod", nonschedulable_pod_id, [""])[
            0
        ]
        updated_allocation = _get_the_only_ip6_address_allocation(updated_pod_dump)

        assert "ip6_address_allocations" not in updated_pod_dump_nonschedulable
        assert initial_allocation["address"] != updated_allocation["address"]
        assert (
            _get_the_only_address_by_fqdn(updated_allocation["persistent_fqdn"])
            == updated_allocation["address"]
        )

        for field_name in ("vlan_id", "labels", "persistent_fqdn", "transient_fqdn"):
            assert initial_allocation[field_name] == updated_allocation[field_name]

        assert (
            initial_pod_dump["status"]["master_spec_timestamp"]
            < updated_pod_dump["status"]["master_spec_timestamp"]
        )
        assert (
            initial_pod_dump_nonschedulable["status"]["master_spec_timestamp"]
            < updated_pod_dump_nonschedulable["status"]["master_spec_timestamp"]
        )

        for pod_dump in [
            initial_pod_dump,
            updated_pod_dump,
            initial_pod_dump_nonschedulable,
            updated_pod_dump_nonschedulable,
        ]:
            del pod_dump["spec"]["ip6_address_requests"]
            del pod_dump["status"]["master_spec_timestamp"]
            if pod_dump["status"].get("ip6_address_allocations", None):
                del pod_dump["status"]["ip6_address_allocations"]

        assert initial_pod_dump_nonschedulable == updated_pod_dump_nonschedulable
        assert initial_pod_dump == updated_pod_dump

    def test_aggregate(self, yp_env):
        cli = create_cli(yp_env)

        pod_set_id = create_pod_set_via_cli(cli)
        pod_ids = []
        memory_limits = [i * 2 ** 20 for i in range(1, 8)]
        for memory_limit in memory_limits:
            attributes = {
                "meta": {"pod_set_id": pod_set_id},
                "spec": {"resource_requests": {"memory_limit": memory_limit}},
            }
            pod_ids.append(
                cli.check_output(["create", "pod", "--attributes", yson.dumps(attributes)])
            )

        result = cli.check_yson_output(
            [
                "aggregate",
                "pod",
                "--group-by",
                'is_prefix([/meta/pod_set_id], "{}")'.format(pod_set_id),
                "--group-by",
                "int64([/status/agent_spec_timestamp]) + 5",
                "--aggregator",
                "avg(int64([/spec/resource_requests/memory_limit]))",
                "--aggregator",
                "max([/meta/creation_time])",
                "--filter",
                '[/meta/pod_set_id] = "{}"'.format(pod_set_id),
                "--no-tabular",
            ]
        )

        assert len(result) == 1
        assert len(result[0]) == 4
        assert result[0][2] == float(sum(memory_limits)) / len(memory_limits)

    def test_object_history_descending_time_order(self, yp_env):
        cli = create_cli(yp_env)
        yp_client = yp_env.yp_client

        create_nodes(yp_client, 1)
        pod_set_id = create_pod_set_via_cli(cli)

        pod_id = create_pod_with_boilerplate(
            yp_client, pod_set_id, spec=dict(enable_scheduling=True)
        )

        wait(lambda: is_pod_assigned(yp_client, pod_id))
        for _ in range(3):
            cli.check_output(["request-pod-eviction", pod_id, "Test"])
            cli.check_output(["abort-pod-eviction", pod_id, "Test"])

        def _get_events(limit, descending_time_order, continuation_token=None):
            query = ["select-object-history", "pod", pod_id, "--limit", str(limit)]
            if descending_time_order:
                query.append("--descending_time_order")
            if continuation_token is not None:
                query.extend(["--continuation-token", continuation_token])

            return cli.check_yson_output(query)

        def _get_all_events(limit, descending_time_order):
            events = []
            continuation_token = None
            current_events = []

            while continuation_token is None or len(current_events) == limit:
                response = _get_events(limit, descending_time_order, continuation_token)
                current_events = response["events"]
                continuation_token = response["continuation_token"]
                events.extend(current_events)

            return events

        limit = 2
        events = _get_all_events(limit, False)
        events_reversed = _get_all_events(limit, True)

        assert len(events) == 8
        assert events == list(reversed(events_reversed))

        event_times = list(map(lambda event: event["time"], events))
        assert event_times == list(sorted(event_times))

    def test_scheduling_hints(self, yp_env):
        cli = create_cli(yp_env)
        yp_client = yp_env.yp_client

        big_node_id = create_nodes(yp_client, 1, cpu_total_capacity=300)[0]
        small_node_id = create_nodes(yp_client, 1, cpu_total_capacity=100)[0]
        pod_set_id = create_pod_set_via_cli(cli)

        pod_id_simple = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(
                enable_scheduling=True, resource_requests=dict(vcpu_guarantee=100, vcpu_limit=100)
            ),
        )

        wait(lambda: is_pod_assigned(yp_client, pod_id_simple))
        assert get_pod_scheduling_status(yp_client, pod_id_simple)["node_id"] == big_node_id

        pod_id_hint = create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(resource_requests=dict(vcpu_guarantee=100, vcpu_limit=100)),
        )

        cli.check_output(["add-scheduling-hint", pod_id_hint, small_node_id, "--strong"])
        cli.check_output(["add-scheduling-hint", pod_id_hint, big_node_id])
        scheduling_hints = yp_client.get_object("pod", pod_id_hint, ["/spec/scheduling/hints"])[0]

        assert len(scheduling_hints) == 2
        assert scheduling_hints[0]["node_id"] == small_node_id
        assert bool(scheduling_hints[0]["strong"]) is True
        assert scheduling_hints[1]["node_id"] == big_node_id
        assert bool(scheduling_hints[1]["strong"]) is False

        cli.check_output(["remove-scheduling-hint", pod_id_hint, scheduling_hints[1]["uuid"]])
        new_scheduling_hints = yp_client.get_object("pod", pod_id_hint, ["/spec/scheduling/hints"])[
            0
        ]

        assert len(new_scheduling_hints) == 1
        assert new_scheduling_hints[0] == scheduling_hints[0]

        yp_client.update_object(
            "pod", pod_id_hint, [dict(path="/spec/enable_scheduling", value=True)]
        )

        wait(lambda: is_pod_assigned(yp_client, pod_id_hint))
        assert get_pod_scheduling_status(yp_client, pod_id_simple)["node_id"] == big_node_id


@pytest.mark.usefixtures("yp_env_configurable")
class TestCliEviction(object):
    # Choosing a pretty small period to optimize tests duration.
    SCHEDULER_LOOP_PERIOD_MILLISECONDS = 1 * 1000

    YP_MASTER_CONFIG = dict(scheduler=dict(loop_period=SCHEDULER_LOOP_PERIOD_MILLISECONDS,))

    def test_pod_eviction(self, yp_env_configurable):
        cli = create_cli(yp_env_configurable)
        yp_client = yp_env_configurable.yp_client

        create_nodes(yp_client, 1)
        pod_set_id = yp_client.create_object("pod_set")
        pod_id = create_pod_with_boilerplate(yp_client, pod_set_id, dict(enable_scheduling=True))
        wait(
            lambda: is_assigned_pod_scheduling_status(get_pod_scheduling_status(yp_client, pod_id))
        )

        get_eviction_state = lambda: cli.check_yson_output(
            ["get", "pod", pod_id, "--selector", "/status/eviction/state"]
        )[0]
        get_eviction_message = lambda: cli.check_yson_output(
            ["get", "pod", pod_id, "--selector", "/status/eviction/message"]
        )[0]

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
        cli.check_output(["acknowledge-eviction", pod_id, "test", "--transaction_id", tx_id])
        assert get_eviction_state() == "requested"
        yp_client.commit_transaction(tx_id)
        assert get_eviction_state() in ("acknowledged", "none")

    def test_evict(self, yp_env_configurable):
        cli = create_cli(yp_env_configurable)
        yp_client = yp_env_configurable.yp_client

        node_id, _, pod_id = prepare_objects(yp_client)

        def get_eviction_state():
            return yp_client.get_object("pod", pod_id, selectors=["/status/eviction/state"])[0]

        def get_node_id():
            return yp_client.get_object("pod", pod_id, selectors=["/spec/node_id"])[0]

        assert node_id == get_node_id()
        assert "none" == get_eviction_state()

        # Disable scheduling.
        cli.check_output(["update-hfsm-state", node_id, "suspected", "Test"])

        cli.check_output(["evict", pod_id])
        wait(lambda: "none" == get_eviction_state())
        assert "" == get_node_id()


@pytest.mark.usefixtures("yp_env_configurable")
class TestCliMaintenance(object):
    # Choosing a pretty small period to optimize tests duration.
    SCHEDULER_LOOP_PERIOD_MILLISECONDS = 1 * 1000

    YP_MASTER_CONFIG = dict(scheduler=dict(loop_period=SCHEDULER_LOOP_PERIOD_MILLISECONDS,))

    def test_add_and_remove_node_alerts(self, yp_env_configurable):
        cli = create_cli(yp_env_configurable)

        yp_client = yp_env_configurable.yp_client

        node_id = create_nodes(yp_client, 1)[0]

        def _get_alerts():
            result = yp_client.get_object("node", node_id, selectors=["/status/alerts"])[0]
            return [] if result == None else result

        assert [] == _get_alerts()

        # Test some fields.
        cli.check_output(["add-node-alert", node_id, "omg"])
        alerts = _get_alerts()
        assert 1 == len(alerts)
        assert "omg" == alerts[0]["type"]
        assert len(alerts[0]["uuid"]) > 0

        # Test alert removing.
        cli.check_output(["remove-node-alert", node_id, alerts[0]["uuid"]])
        assert [] == _get_alerts()

    def test_acknowledge_and_renounce_pod_maintenance(self, yp_env_configurable):
        cli = create_cli(yp_env_configurable)
        yp_client = yp_env_configurable.yp_client

        node_id, pod_set_id, pod_id = prepare_objects(yp_client)

        def _get_maintenance():
            return yp_client.get_object("pod", pod_id, selectors=["/status/maintenance"])[0]

        yp_client.update_hfsm_state(
            node_id, "prepare_maintenance", "Test", maintenance_info=dict(id="aba"),
        )

        wait(lambda: _get_maintenance().get("state") == "requested")

        maintenance = _get_maintenance()
        assert maintenance["info"]["id"] == "aba"
        assert len(maintenance["info"]["uuid"]) > 0

        # Acknowledgement of requested maintenance.
        cli.check_output(["acknowledge-pod-maintenance", pod_id])
        new_maintenance = _get_maintenance()
        assert new_maintenance["state"] == "acknowledged"
        assert maintenance["info"] == new_maintenance["info"]

        # Renouncement of acknowledged maintenance.
        cli.check_output(["renounce-pod-maintenance", pod_id])
        new_maintenance = _get_maintenance()
        assert maintenance["state"] == new_maintenance["state"]
        assert maintenance["info"] == new_maintenance["info"]
