from original_tests.yt.yt.tests.integration.tests.controller.test_map_reduce_operation \
    import TestSchedulerMapReduceCommands as BaseTestMapReduceCommands


class TestMapReduceCommandsCompatUpToCA(BaseTestMapReduceCommands):
    ARTIFACT_COMPONENTS = {
        "21_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }
