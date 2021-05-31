from original_tests.yt.yt.tests.integration.tests.test_scheduler_map \
    import TestSchedulerMapCommands as BaseTestMapCommands


class TestMapCommandsCompatUpToCA(BaseTestMapCommands):
    ARTIFACT_COMPONENTS = {
        "21_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }
