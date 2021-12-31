from yt_env_setup import YTEnvSetup

from yt_commands import authors, get, ls, exists, wait

##################################################################


class TestQueues(YTEnvSetup):
    NUM_QUEUE_AGENTS = 1

    @authors("max42")
    def test_queue_agents(self):
        wait(lambda: exists("//sys/queue_agents/instances") and get("//sys/queue_agents/instances/@count") > 0)
        agent_id = ls("//sys/queue_agents/instances")[0]
        get("//sys/queue_agents/instances/" + agent_id + "/orchid")
