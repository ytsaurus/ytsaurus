from original_tests.yt.yt.tests.integration.tests.controller.test_remote_copy_operation \
    import TestSchedulerRemoteCopyCommands as BaseTestRemoteCopyCommands


class TestRemoteCopyCommandsCompatUpNewCA(BaseTestRemoteCopyCommands):
    ARTIFACT_COMPONENTS = {
        "22_1": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestRemoteCopyCommandsCompatNewNodes(BaseTestRemoteCopyCommands):
    ARTIFACT_COMPONENTS = {
        "22_1": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }
