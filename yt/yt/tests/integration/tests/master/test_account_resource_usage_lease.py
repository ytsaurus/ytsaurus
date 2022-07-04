from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors,
    exists, remove, get, set, ls,
    get_account_disk_space, create_account, create_user, create_account_resource_usage_lease,
    start_transaction, abort_transaction, commit_transaction)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest


##################################################################


def update_account_resource_usage_lease(lease_id, resources, **kwargs):
    set("#{}/@resource_usage".format(lease_id), resources, **kwargs)


class TestAccountResourceUsageLease(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    def _finish_transaction(self, tx, action):
        if action == "commit":
            commit_transaction(tx)
        else:  # "abort"
            abort_transaction(tx)

    @authors("ignat")
    def test_simple(self):
        create_account("a")

        with pytest.raises(YtError):
            create_account_resource_usage_lease(account="a", transaction_id="0-0-0-0")

        tx = start_transaction()

        with pytest.raises(YtError):
            create_account_resource_usage_lease(account="non_existing", transaction_id=tx)

        lease_id = create_account_resource_usage_lease(account="a", transaction_id=tx)
        assert exists("#" + lease_id)
        assert get("#{}/@transaction_id".format(lease_id)) == tx
        assert get("#{}/@account".format(lease_id)) == "a"

        remove("#" + lease_id)
        assert not exists("#" + lease_id)

    @authors("ignat")
    @pytest.mark.parametrize("transaction_action", ["abort", "commit"])
    def test_transaction_finish(self, transaction_action):
        create_account("a")

        tx = start_transaction()
        lease_id = create_account_resource_usage_lease(account="a", transaction_id=tx)
        assert exists("#" + lease_id)
        assert get("#{}/@account_resource_usage_lease_ids".format(tx)) == [lease_id]
        self._finish_transaction(tx, transaction_action)

        assert not exists("#" + lease_id)

    @authors("ignat")
    def test_multiple_leases(self):
        create_account("a")
        create_account("b")

        tx = start_transaction()
        lease_id_a1 = create_account_resource_usage_lease(account="a", transaction_id=tx)
        assert exists("#" + lease_id_a1)
        lease_id_a2 = create_account_resource_usage_lease(account="a", transaction_id=tx)
        assert exists("#" + lease_id_a2)
        lease_id_b = create_account_resource_usage_lease(account="b", transaction_id=tx)
        assert exists("#" + lease_id_b)

        assert_items_equal(
            get("#{}/@account_resource_usage_lease_ids".format(tx)),
            [lease_id_a1, lease_id_a2, lease_id_b])

        update_account_resource_usage_lease(
            lease_id_a1,
            resources={"disk_space_per_medium": {"default": 200}})
        update_account_resource_usage_lease(
            lease_id_a2,
            resources={"disk_space_per_medium": {"default": 300}})
        update_account_resource_usage_lease(
            lease_id_b,
            resources={"disk_space_per_medium": {"default": 400}})

        assert get_account_disk_space("a") == 500
        assert get_account_disk_space("b") == 400

        tx_resource_usage = get("//sys/transactions/{0}/@resource_usage".format(tx))
        assert tx_resource_usage["a"]["disk_space_per_medium"]["default"] == 500
        assert tx_resource_usage["b"]["disk_space_per_medium"]["default"] == 400

        commit_transaction(tx)

        for lease_id in (lease_id_a1, lease_id_a2, lease_id_b):
            assert not exists("#" + lease_id)

        assert get_account_disk_space("a") == 0
        assert get_account_disk_space("b") == 0

    @authors("ignat")
    @pytest.mark.parametrize("transaction_action", ["abort", "commit"])
    def test_update_resources(self, transaction_action):
        create_account("a")
        assert get_account_disk_space("a") == 0

        tx = start_transaction()
        lease_id = create_account_resource_usage_lease(account="a", transaction_id=tx)
        assert exists("#" + lease_id)
        assert get_account_disk_space("a") == 0

        update_account_resource_usage_lease(lease_id, {"disk_space_per_medium": {"default": 1000}})

        assert get_account_disk_space("a") == 1000
        tx_resource_usage = get("//sys/transactions/{0}/@resource_usage".format(tx))
        assert tx_resource_usage["a"]["disk_space_per_medium"]["default"] == 1000

        self._finish_transaction(tx, transaction_action)

        assert get_account_disk_space("a") == 0

    @authors("ignat")
    def test_multiple_update_resources(self):
        create_account("a")
        assert get_account_disk_space("a") == 0

        tx = start_transaction()
        lease_id = create_account_resource_usage_lease(account="a", transaction_id=tx)
        assert exists("#" + lease_id)
        assert get_account_disk_space("a") == 0

        update_account_resource_usage_lease(
            lease_id,
            resources={"disk_space_per_medium": {"default": 1000}})

        assert get_account_disk_space("a") == 1000
        tx_resource_usage = get("//sys/transactions/{0}/@resource_usage".format(tx))
        assert tx_resource_usage["a"]["disk_space_per_medium"]["default"] == 1000

        update_account_resource_usage_lease(
            lease_id,
            resources={"disk_space_per_medium": {"default": 500}})

        assert get_account_disk_space("a") == 500
        tx_resource_usage = get("//sys/transactions/{0}/@resource_usage".format(tx))
        assert tx_resource_usage["a"]["disk_space_per_medium"]["default"] == 500

        commit_transaction(tx)

        assert get_account_disk_space("a") == 0

    @authors("ignat")
    def test_incorrect_update_resources(self):
        create_account("a")
        assert get_account_disk_space("a") == 0

        tx = start_transaction()
        lease_id = create_account_resource_usage_lease(account="a", transaction_id=tx)

        # Negative resources.
        with pytest.raises(YtError):
            update_account_resource_usage_lease(
                lease_id,
                resources={"disk_space_per_medium": {"default": -1000}})

        commit_transaction(tx)

        # Missing lease.
        with pytest.raises(YtError):
            update_account_resource_usage_lease(
                lease_id,
                resources={"disk_space_per_medium": {"default": 1000}})

    @authors("ignat")
    def test_account_overcommit(self):
        create_account("a")
        assert get_account_disk_space("a") == 0

        set("//sys/accounts/a/@resource_limits/disk_space_per_medium", {"default": 1000})

        tx = start_transaction()
        lease_id = create_account_resource_usage_lease(account="a", transaction_id=tx)

        update_account_resource_usage_lease(
            lease_id,
            resources={"disk_space_per_medium": {"default": 500}})
        assert get_account_disk_space("a") == 500

        with pytest.raises(YtError):
            update_account_resource_usage_lease(
                lease_id,
                resources={"disk_space_per_medium": {"default": 1001}})

        commit_transaction(tx)

    @authors("ignat")
    def test_permissions(self):
        create_user("test_user")
        create_account("a")

        tx = start_transaction()
        lease_id = create_account_resource_usage_lease(account="a", transaction_id=tx)

        with pytest.raises(YtError):
            update_account_resource_usage_lease(
                lease_id,
                resources={"disk_space_per_medium": {"default": 500}},
                authenticated_user="test_user")

    @authors("ignat")
    def test_incorrect_resources(self):
        create_account("a")
        assert get_account_disk_space("a") == 0

        tx = start_transaction()
        lease_id = create_account_resource_usage_lease(account="a", transaction_id=tx)
        assert exists("#" + lease_id)
        assert get_account_disk_space("a") == 0

        with pytest.raises(YtError):
            update_account_resource_usage_lease(lease_id, resources={"node_count": 1})

        with pytest.raises(YtError):
            update_account_resource_usage_lease(lease_id, resources={"chunk_count": 1})

        with pytest.raises(YtError):
            update_account_resource_usage_lease(lease_id, resources={"tablet_count": 1})

        with pytest.raises(YtError):
            update_account_resource_usage_lease(lease_id, resources={"tablet_static_memory": 1024 * 1024})

        update_account_resource_usage_lease(
            lease_id,
            resources={"disk_space_per_medium": {"default": 500}})

        commit_transaction(tx)

    @authors("ignat")
    def test_virtual_map(self):
        create_account("a")

        tx = start_transaction()

        lease_id_a1 = create_account_resource_usage_lease(account="a", transaction_id=tx)

        assert exists("#" + lease_id_a1)
        assert get("#{}/@resource_usage/node_count".format(lease_id_a1)) == 0

        assert ls("//sys/account_resource_usage_leases") == [lease_id_a1]

        assert exists("//sys/account_resource_usage_leases/" + lease_id_a1)
        assert get("//sys/account_resource_usage_leases/{}/@resource_usage/node_count".format(lease_id_a1)) == 0

        lease_id_a2 = create_account_resource_usage_lease(account="a", transaction_id=tx)

        assert exists("#" + lease_id_a2)
        assert get("#{}/@resource_usage/node_count".format(lease_id_a2)) == 0

        assert_items_equal(ls("//sys/account_resource_usage_leases"), [lease_id_a1, lease_id_a2])

        assert exists("//sys/account_resource_usage_leases/" + lease_id_a2)
        assert get("//sys/account_resource_usage_leases/{}/@resource_usage/node_count".format(lease_id_a2)) == 0
