from original_tests.yt.yt.tests.integration.controller.test_sort_operation \
    import TestSchedulerSortCommands as BaseTestSortCommands


class TestSortCommandsCompatNewCA(BaseTestSortCommands):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestSortCommandsCompatNewNodes(BaseTestSortCommands):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }
