from original_tests.yt.yt.tests.integration.controller.test_merge_operation \
    import TestSchedulerMergeCommands as BaseTestMergeCommands


class TestMergeCommandsCompatNewCA(BaseTestMergeCommands):
    ARTIFACT_COMPONENTS = {
        "23_1": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestMergeCommandsCompatNewNodes(BaseTestMergeCommands):
    ARTIFACT_COMPONENTS = {
        "23_1": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }
