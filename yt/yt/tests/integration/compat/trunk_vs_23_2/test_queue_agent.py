from original_tests.yt.yt.tests.integration.queues.test_queue_agent \
    import \
    TestReplicatedTableObjects as BaseTestReplicatedTableObjects, \
    TestMultiClusterReplicatedTableObjects as BaseTestMultiClusterReplicatedTableObjects


class TestReplicatedTableObjectsCompatOldMasters(BaseTestReplicatedTableObjects):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master"],
        "trunk": ["node", "job-proxy", "exec", "tools", "scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestMultiClusterReplicatedTableObjectsCompatOldMasters(BaseTestMultiClusterReplicatedTableObjects):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master"],
        "trunk": ["node", "job-proxy", "exec", "tools", "scheduler", "controller-agent", "proxy", "http-proxy"],
    }
