from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, start_shuffle, write_shuffle_data, read_shuffle_data, start_transaction,
    abort_transaction, commit_transaction, wait, raises_yt_error, ls, create_user,
    create_account
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

        # Check that active_shuffle_count_sensor actually works
        wait(lambda: active_shuffle_count_sensor.get() == 1, iter=300, sleep_backoff=0.1)

        if abort:
            abort_transaction(parent_transaction)
        else:
            commit_transaction(parent_transaction)

        wait(lambda: active_shuffle_count_sensor.get() == 0, iter=300, sleep_backoff=0.1)

        with raises_yt_error(f'Shuffle with id "{shuffle_handle["transaction_id"]}" does not exist'):
            read_shuffle_data(shuffle_handle, 1)

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

        with raises_yt_error('User "u" has been denied "Use" access to account "a"'):
            start_shuffle("a", partition_count=2, parent_transaction_id=parent_transaction, authenticated_user="u")
