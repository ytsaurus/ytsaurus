from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, start_shuffle, write_shuffle_data, read_shuffle_data, start_transaction,
    abort_transaction, commit_transaction, wait, raises_yt_error, ls, create_user,
    create_account, create_domestic_medium, get_account_disk_space_limit,
    set_account_disk_space_limit, get_account_disk_space, get_chunks, get
)

from yt_helpers import profiler_factory

import pytest

##################################################################


class TestShuffleService(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_RPC_PROXIES = 1

    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "enable_shuffle_service": True,
    }

    STORE_LOCATION_COUNT = 2

    NON_DEFAULT_MEDIUM = "hdd1"

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        assert len(config["data_node"]["store_locations"]) == 2

        config["data_node"]["store_locations"][0]["medium_name"] = "default"
        config["data_node"]["store_locations"][1]["medium_name"] = cls.NON_DEFAULT_MEDIUM

    @classmethod
    def setup_class(cls):
        super(TestShuffleService, cls).setup_class()
        create_domestic_medium(cls.NON_DEFAULT_MEDIUM)

    @authors("apollo1321")
    @pytest.mark.parametrize("abort", [False, True])
    def test_simple(self, abort):
        proxy = ls("//sys/rpc_proxies")[0]
        active_shuffle_count_sensor = profiler_factory().at_rpc_proxy(proxy).gauge("shuffle_manager/active")

        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle("intermediate", partition_count=11, parent_transaction_id=parent_transaction)

        for i in range(10):
            rows = [{"key": i, "value": i * 10 + j} for j in range(10)]
            write_shuffle_data(shuffle_handle, "key", rows)

        for i in range(10):
            rows = [{"key": i, "value": i * 10 + j} for j in range(10)]
            assert read_shuffle_data(shuffle_handle, i) == rows

        assert read_shuffle_data(shuffle_handle, 10) == []

        # Check that active_shuffle_count_sensor actually works.
        wait(lambda: active_shuffle_count_sensor.get() == 1, iter=300, sleep_backoff=0.1)

        if abort:
            abort_transaction(parent_transaction)
        else:
            commit_transaction(parent_transaction)

        wait(lambda: active_shuffle_count_sensor.get() == 0, iter=300, sleep_backoff=0.1)

        with raises_yt_error(f'Shuffle with id {shuffle_handle["transaction_id"]} does not exist'):
            read_shuffle_data(shuffle_handle, 1)

    @authors("apollo1321")
    def test_different_partition_columns(self):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle("intermediate", partition_count=11, parent_transaction_id=parent_transaction)

        # Check that a negative partition index is not accepted.
        with raises_yt_error("Received negative partition index -1"):
            write_shuffle_data(shuffle_handle, "key1", [{"key1": -1}])

        rows = [{"key1": 0, "key2": -1}, {"key1": -1, "key2": 0}]
        write_shuffle_data(shuffle_handle, "key1", rows[0])
        write_shuffle_data(shuffle_handle, "key2", rows[1])

        assert read_shuffle_data(shuffle_handle, 0) == rows

    @authors("apollo1321")
    def test_write_unsorted_rows(self):
        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle("intermediate", partition_count=11, parent_transaction_id=parent_transaction)

        # Verify that the writer does not enforce any specific sort order on the rows.
        rows = [
            {"key1": 5, "key2": -1},
            {"key1": 0, "key2": -10},
            {'key1': 4, "key2": -42},
            {'key1': 4, "key2": 10},
        ]
        write_shuffle_data(shuffle_handle, "key1", rows)

        assert read_shuffle_data(shuffle_handle, 0) == [rows[1]]
        assert read_shuffle_data(shuffle_handle, 5) == [rows[0]]
        assert read_shuffle_data(shuffle_handle, 4) == [rows[2], rows[3]]

    @authors("apollo1321")
    def test_invalid_arguments(self):
        with raises_yt_error('Parent transaction id is null'):
            start_shuffle("root", partition_count=2, parent_transaction_id="0-0-0-0")

        with raises_yt_error('Unknown master cell tag'):
            start_shuffle("root", partition_count=2, parent_transaction_id="1-2-3-4")

        parent_transaction = start_transaction(timeout=60000)

        with raises_yt_error('Node has no child with key "a"'):
            start_shuffle("a", partition_count=2, parent_transaction_id=parent_transaction)

        create_user("u")
        create_account("a")

        with raises_yt_error('User "u" has been denied "use" access to account "a"'):
            start_shuffle("a", partition_count=2, parent_transaction_id=parent_transaction, authenticated_user="u")

    @authors("apollo1321")
    def test_store_options(self):
        disk_space_limit = get_account_disk_space_limit("intermediate", "default")
        set_account_disk_space_limit("intermediate", disk_space_limit, self.NON_DEFAULT_MEDIUM)

        parent_transaction = start_transaction(timeout=60000)

        shuffle_handle = start_shuffle(
            "intermediate",
            partition_count=11,
            parent_transaction_id=parent_transaction,
            replication_factor=5,
            medium_name=self.NON_DEFAULT_MEDIUM)

        rows = [{"key1": 0, "key2": -1}, {"key1": 1, "key2": 0}]
        write_shuffle_data(shuffle_handle, "key1", rows[1])
        write_shuffle_data(shuffle_handle, "key1", rows[0])

        assert read_shuffle_data(shuffle_handle, 0) == [rows[0]]
        assert read_shuffle_data(shuffle_handle, 1) == [rows[1]]

        assert get_account_disk_space("intermediate", "default") == 0
        assert get_account_disk_space("intermediate", self.NON_DEFAULT_MEDIUM) > 0

        chunks = get_chunks()
        assert len(chunks) > 0
        for chunk in chunks:
            media = get(f"#{chunk}/@media")
            assert len(media) == 1
            assert media[self.NON_DEFAULT_MEDIUM]["replication_factor"] == 5

        commit_transaction(parent_transaction)
