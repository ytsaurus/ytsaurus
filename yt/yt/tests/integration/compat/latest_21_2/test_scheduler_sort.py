from original_tests.yt.yt.tests.integration.tests.scheduler.test_scheduler_sort \
    import TestSchedulerSortCommands as BaseTestSortCommands


class TestSortCommandsCompatUpToCA(BaseTestSortCommands):
    ARTIFACT_COMPONENTS = {
        "21_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }
