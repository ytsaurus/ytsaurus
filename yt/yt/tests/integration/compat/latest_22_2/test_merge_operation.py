from original_tests.yt.yt.tests.integration.tests.controller.test_merge_operation \
    import TestSchedulerMergeCommands as BaseTestMergeCommands


class TestMergeCommandsCompatNewCA(BaseTestMergeCommands):
    ARTIFACT_COMPONENTS = {
        "22_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestMergeCommandsCompatNewNodes(BaseTestMergeCommands):
    UPLOAD_DEBUG_ARTIFACT_CHUNKS = True

    ARTIFACT_COMPONENTS = {
        "22_2": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }
