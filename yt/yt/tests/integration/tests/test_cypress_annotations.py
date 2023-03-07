from yt_env_setup import YTEnvSetup, unix_only, patch_porto_env_only, wait
from yt_commands import *

class TestCypressAnnotations(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SECONDARY_MASTER_CELLS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    DELTA_SCHEDULER_CONFIG = {
        "cypress_annotations" : { "whoami" : "scheduler" }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "cypress_annotations" : { "whoami" : "controller_agent" }
    }

    DELTA_MASTER_CONFIG = {
        "cypress_annotations" : { "whoami" : "master" },
        "world_initializer": {
            "init_retry_period": 100,
            "update_period": 100,
        },
    }

    DELTA_NODE_CONFIG = {
        "cypress_annotations" : { "whoami" : "node" }
    }

    DELTA_PROXY_CONFIG = {
        "cypress_annotations" : { "whoami" : "proxy" }
    }

    DELTA_RPC_PROXY_CONFIG = {
        "cypress_annotations" : { "whoami" : "rpc_proxy" }
    }

    @authors("prime", "psushin")
    def test_annotations(self):
        n = ls("//sys/cluster_nodes")[0]
        assert "node" == get("//sys/cluster_nodes/{0}/@annotations/whoami".format(n))

        s = ls("//sys/scheduler/instances")[0]
        assert "scheduler" == get("//sys/scheduler/instances/{0}/@annotations/whoami".format(s))

        ca = ls("//sys/controller_agents/instances")[0]
        assert "controller_agent" == get("//sys/controller_agents/instances/{0}/@annotations/whoami".format(ca))

        p = ls("//sys/proxies")[0]
        assert "proxy" == get("//sys/proxies/{}/@annotations/whoami".format(p))

        rp = ls("//sys/rpc_proxies")[0]
        assert "rpc_proxy" == get("//sys/rpc_proxies/{}/@annotations/whoami".format(rp))

        pm = ls("//sys/primary_masters")[0]
        wait(lambda: exists("//sys/primary_masters/{0}/@annotations".format(pm)))
        assert "master" == get("//sys/primary_masters/{0}/@annotations/whoami".format(pm))

        cell = ls("//sys/secondary_masters")[0]
        sm = ls("//sys/secondary_masters/" + cell)[0]
        wait(lambda: exists("//sys/secondary_masters/{0}/{1}/@annotations".format(cell, sm)))
        assert "master" == get("//sys/secondary_masters/{0}/{1}/@annotations/whoami".format(cell, sm))
