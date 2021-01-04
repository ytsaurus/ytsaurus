from original_tests.yt.yt.tests.integration.tests.test_scheduler_join_reduce \
    import TestSchedulerJoinReduceCommands as BaseTestJoinReduceCommands


class TestJoinReduceCommandsCompatUpToCA(BaseTestJoinReduceCommands):
    ARTIFACT_COMPONENTS = {
        "20_3": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }
