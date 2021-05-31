from original_tests.yt.yt.tests.integration.tests.test_scheduler_merge \
    import TestSchedulerMergeCommands as BaseTestMergeCommands


class TestMergeCommandsCompatUpToCA(BaseTestMergeCommands):
    ARTIFACT_COMPONENTS = {
        "21_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }
