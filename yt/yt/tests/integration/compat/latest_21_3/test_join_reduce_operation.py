from original_tests.yt.yt.tests.integration.tests.controller.test_join_reduce_operation \
    import TestSchedulerJoinReduceCommands as BaseTestJoinReduceCommands


class TestJoinReduceCommandsCompatUpToCA(BaseTestJoinReduceCommands):
    ARTIFACT_COMPONENTS = {
        "21_3": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }
