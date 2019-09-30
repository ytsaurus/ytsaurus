from .conftest import (
    ZERO_RESOURCE_REQUESTS,
    create_nodes,
    create_pod_with_boilerplate,
    create_pod_set_with_quota,
)

from yp.common import YtResponseError, wait
from yp.local import set_account_infinite_resource_limits

from yt.environment.helpers import assert_items_equal

from yt.packages.six.moves import zip

import pytest


@pytest.mark.usefixtures("yp_env")
class TestAccounts(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("account", attributes={"meta": {"id": "a"}})
        assert yp_client.get_object("account", "a", selectors=["/meta/id"])[0] == "a"

        yp_client.create_object("account", attributes={"meta": {"id": "b"}})
        assert yp_client.get_object("account", "b", selectors=["/meta/id"])[0] == "b"

        assert yp_client.get_object("account", "a", selectors=["/spec/parent_id"])[0] == ""
        yp_client.update_object("account", "a", set_updates=[{"path": "/spec/parent_id", "value": "b"}])
        assert yp_client.get_object("account", "a", selectors=["/spec/parent_id"])[0] == "b"

        yp_client.remove_object("account", "b")
        assert yp_client.get_object("account", "a", selectors=["/spec/parent_id"])[0] == ""

    def test_builtin_accounts(self, yp_env):
        yp_client = yp_env.yp_client

        accounts = [x[0] for x in yp_client.select_objects("account", selectors=["/meta/id"])]
        assert_items_equal(accounts, ["tmp"])

        for account in accounts:
            with pytest.raises(YtResponseError):
                yp_client.remove_object("account", account)

    def test_cannot_set_null_account(self, yp_env):
        yp_client = yp_env.yp_client

        pod_set_id = yp_client.create_object("pod_set")
        with pytest.raises(YtResponseError):
            yp_client.update_object("pod_set", pod_set_id, set_updates=[{"path": "/spec/account_id", "value": ""}])

    def test_cannot_create_with_null_account(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YtResponseError):
            yp_client.create_object("pod_set", attributes={
                    "spec": {
                        "account_id": ""
                    }
                })
    def test_must_have_use_permission1(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account", attributes={
            "spec": {}
        })

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            def create_pod_set():
                yp_client1.create_object("pod_set", attributes={
                    "spec": {"account_id": account_id}
                })

            with pytest.raises(YtResponseError):
                create_pod_set()

            yp_client.update_object("account", account_id, set_updates=[
                {"path": "/meta/acl/end", "value": {"action": "allow", "permissions": ["use"], "subjects": ["u"]}}
            ])

            create_pod_set()

    def test_must_have_use_permission2(self, yp_env):
        yp_client = yp_env.yp_client

        account_id = yp_client.create_object("account", attributes={
            "spec": {}
        })

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client1:
            pod_set_id = yp_client1.create_object("pod_set")

            def create_pod():
                yp_client1.create_object("pod", attributes={
                    "meta": {"pod_set_id": pod_set_id},
                    "spec": {
                        "account_id": account_id,
                        "resource_requests": ZERO_RESOURCE_REQUESTS
                    }
                })

            with pytest.raises(YtResponseError):
                create_pod()

            yp_client.update_object("account", account_id, set_updates=[
                {"path": "/meta/acl/end", "value": {"action": "allow", "permissions": ["use"], "subjects": ["u"]}}
            ])

            create_pod()

    def test_null_account_id_yp_717(self, yp_env):
        yp_client = yp_env.yp_client

        yp_client.create_object("user", attributes={"meta": {"id": "u"}})
        yp_env.sync_access_control()

        with yp_env.yp_instance.create_client(config={"user": "u"}) as yp_client_with_user:
            pod_set_id = yp_client_with_user.create_object("pod_set")
            yp_client_with_user.create_object("pod", attributes={
                "meta": {"pod_set_id": pod_set_id},
                "spec": {
                    "resource_requests": ZERO_RESOURCE_REQUESTS,
                    "account_id": ""
                }
            })

    def test_hierarchical_accounting(self, yp_env):
        yp_client = yp_env.yp_client

        parent_account_id = "parent_account"
        child_account_ids = ("first_child_account", "second_child_account")

        yp_client.create_object("account", attributes=dict(meta=dict(id=parent_account_id)))
        for child_account_id in child_account_ids:
            yp_client.create_object(
                "account",
                attributes=dict(
                    meta=dict(id=child_account_id),
                    spec=dict(parent_id=parent_account_id),
                ),
            )

        for account_id in (parent_account_id, ) + child_account_ids:
            set_account_infinite_resource_limits(yp_client, account_id)

        pod_set_ids = ("first_pod_set", "second_pod_set")
        assert len(pod_set_ids) == len(child_account_ids)
        for pod_set_id, child_account_id in zip(pod_set_ids, child_account_ids):
            yp_client.create_object(
                "pod_set",
                attributes=dict(
                    meta=dict(id=pod_set_id),
                    spec=dict(account_id=child_account_id, node_segment_id="default"),
                ),
            )

        def get_account_cpu_usage(account_id):
            return yp_client.get_object(
                "account",
                account_id,
                selectors=["/status/resource_usage"],
            )[0].get("per_segment", {}).get("default", {}).get("cpu", {}).get("capacity", 0)

        cpu_guarantee = 100
        pod_spec = dict(enable_scheduling=True, resource_requests=dict(vcpu_guarantee=cpu_guarantee))

        create_nodes(yp_client, 1)

        wait(lambda: get_account_cpu_usage(parent_account_id) == 0)
        assert get_account_cpu_usage(child_account_ids[0]) == 0
        assert get_account_cpu_usage(child_account_ids[1]) == 0

        create_pod_with_boilerplate(yp_client, pod_set_ids[0], pod_spec)

        wait(lambda: get_account_cpu_usage(parent_account_id) == cpu_guarantee)
        assert get_account_cpu_usage(child_account_ids[0]) == cpu_guarantee
        assert get_account_cpu_usage(child_account_ids[1]) == 0

        create_pod_with_boilerplate(yp_client, pod_set_ids[1], pod_spec)

        wait(lambda: get_account_cpu_usage(parent_account_id) == 2 * cpu_guarantee)
        assert get_account_cpu_usage(child_account_ids[0]) == cpu_guarantee
        assert get_account_cpu_usage(child_account_ids[1]) == cpu_guarantee


@pytest.mark.usefixtures("yp_env")
class TestAccountQuota(object):
    def test_gpu_limits(self, yp_env):
        yp_client = yp_env.yp_client
        pod_set_id, account_id, node_segment_id = create_pod_set_with_quota(
            yp_client,
            gpu_quota=dict(v100=2, k200=1))
        with pytest.raises(YtResponseError):
            create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                spec=dict(
                    gpu_requests=[
                        dict(
                            id="gpu{}".format(i),
                            model="v100",
                            min_memory=2 ** 10,
                        )
                        for i in range(3)
                    ]
                ))

        make_pod = lambda gpu_model: create_pod_with_boilerplate(
            yp_client,
            pod_set_id,
            spec=dict(
                enable_scheduling=True,
                gpu_requests=[
                    dict(
                        id="mygpu",
                        model=gpu_model,
                        min_memory=2 ** 10,
                    )
                ]
            ))

        for _ in range(2):
            make_pod("v100")
        wait(lambda: yp_client.get_object("account", account_id,
             selectors=["/status/resource_usage/per_segment/{}/gpu_per_model/v100/capacity"
                        .format(node_segment_id)])[0] == 2)
        with pytest.raises(YtResponseError):
            make_pod("v100")

        make_pod("k200")
        wait(lambda: yp_client.get_object("account", account_id,
             selectors=["/status/resource_usage/per_segment/{}/gpu_per_model/k200/capacity"
                        .format(node_segment_id)])[0] == 1)
        with pytest.raises(YtResponseError):
            make_pod("k200")

        with pytest.raises(YtResponseError):
            make_pod("z300")

    def test_network_limits(self, yp_env):
        yp_client = yp_env.yp_client
        pod_set_id, account_id, node_segment_id = create_pod_set_with_quota(
            yp_client,
            bandwidth_quota=2 ** 10)

        make_pod = lambda bandwidth, enable_scheduling=True: create_pod_with_boilerplate(
                yp_client,
                pod_set_id,
                spec=dict(
                    enable_scheduling=enable_scheduling,
                    resource_requests=dict(network_bandwidth_guarantee=bandwidth)
                ),
            )

        with pytest.raises(YtResponseError):
            make_pod(2 ** 10 + 1, False)

        for _ in range(3):
            make_pod(2 ** 8)

        wait(lambda: yp_client.get_object("account", account_id,
             selectors=["/status/resource_usage/per_segment/{}/network/bandwidth"
                        .format(node_segment_id)])[0] == 3 * 2 ** 8)

        with pytest.raises(YtResponseError):
            make_pod(2**8 + 1)
