from yt_env_setup import YTEnvSetup, unix_only, patch_porto_env_only, wait
from yt_commands import *

class TestAnnotations(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "cypress_annotations" : { "whoami" : "scheduler" }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "cypress_annotations" : { "whoami" : "controller_agent" }
    }

    DELTA_MASTER_CONFIG = {
        "cypress_annotations" : { "whoami" : "master" }
    }

    DELTA_NODE_CONFIG = {
        "cypress_annotations" : { "whoami" : "node" }
    }

    def test_annotations(self):
        n = yt.get("//sys/nodes")
        assert "node" == yt.get("//sys/nodes/{0}/@annotations/whoami")

        m = yt.get("//sys/primary_masters")
        assert "master" == yt.get("//sys/primary_masters/{0}/orchid/config/cypress_annotations".format(m))

        s = yt.get("//sys/scheduler/instances")
        assert "scheduler" == yt.get("//sys/scheduler/instances/{0}/orchid/config/cypress_annotations".format(s))

        ca = yt.get("//sys/controller_agents/instances")
        assert "controller_agent" == yt.get("//sys/scheduler/instances/{0}/orchid/config/cypress_annotations".format(ca))


