from original_tests.yt.yt.tests.integration.tests.controller.test_map_operation \
    import TestSchedulerMapCommands as BaseTestMapCommands


class TestMapCommandsCompatUpToCA(BaseTestMapCommands):
    ARTIFACT_COMPONENTS = {
        "21_3": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }


class TestMapCommandsCompatNewNodes(BaseTestMapCommands):
    UPLOAD_DEBUG_ARTIFACT_CHUNKS = True

    ARTIFACT_COMPONENTS = {
        "21_3": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }
