from original_tests.yt.yt.tests.integration.tests.controller.test_sort_operation \
    import TestSchedulerSortCommands as BaseTestSortCommands


class TestSortCommandsCompatUpToCA(BaseTestSortCommands):
    ARTIFACT_COMPONENTS = {
        "21_3": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }
