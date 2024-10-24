from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors,
    start_shuffle, finish_shuffle, write_shuffle_data, read_shuffle_data
)


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
    def test_simple(self):
        shuffle_handle = start_shuffle("intermediate", partition_count=10)

        rows = []
        for i in range(100):
            rows.append([{"key": i % 10, "value": i}])

        for i in range(10):
            rows = [{"key": i, "value": i * 10 + j} for j in range(10)]
            write_shuffle_data(shuffle_handle, "key", rows)

        for i in range(10):
            rows = [{"key": i, "value": i * 10 + j} for j in range(10)]
            assert read_shuffle_data(shuffle_handle, i) == rows

        finish_shuffle(shuffle_handle)
