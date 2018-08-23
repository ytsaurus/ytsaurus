from yt_commands import *

from yt_env_setup import YTEnvSetup

from time import sleep

class TestRpcProxy(YTEnvSetup):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    def test_non_sticky_transactions_dont_stick(self):
        tx = start_transaction(timeout=1000)
        wait(lambda: not exists("//sys/transactions/" + tx))
