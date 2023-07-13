from original_tests.yt.yt.tests.integration.queues.test_queue_agent \
    import TestReplicatedTableObjects as BaseTestReplicatedTableObjects


class TestReplicatedTableObjectsCompatOldMasters(BaseTestReplicatedTableObjects):
    ARTIFACT_COMPONENTS = {
        "23_1": ["master"],
        "trunk": ["node", "job-proxy", "exec", "tools", "scheduler", "controller-agent", "proxy", "http-proxy"],
    }
