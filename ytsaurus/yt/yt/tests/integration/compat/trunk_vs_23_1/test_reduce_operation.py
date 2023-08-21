from original_tests.yt.yt.tests.integration.controller.test_reduce_operation \
    import TestSchedulerReduceCommands as BaseTestReduceCommands


class TestReduceCommandsCompatNewCA(BaseTestReduceCommands):
    ARTIFACT_COMPONENTS = {
        "23_1": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestReduceCommandsCompatNewNodes(BaseTestReduceCommands):
    ARTIFACT_COMPONENTS = {
        "23_1": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }
