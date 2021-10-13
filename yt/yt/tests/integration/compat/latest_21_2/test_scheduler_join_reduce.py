from original_tests.yt.yt.tests.integration.tests.scheduler.test_scheduler_join_reduce \
    import TestSchedulerJoinReduceCommands as BaseTestJoinReduceCommands


class TestJoinReduceCommandsCompatUpToCA(BaseTestJoinReduceCommands):
    ARTIFACT_COMPONENTS = {
        "21_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }
