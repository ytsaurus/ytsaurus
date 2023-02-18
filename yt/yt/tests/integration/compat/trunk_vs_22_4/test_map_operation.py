from original_tests.yt.yt.tests.integration.controller.test_map_operation \
    import TestSchedulerMapCommands as BaseTestMapCommands

from yt_commands import authors


class TestMapCommandsCompatNewCA(BaseTestMapCommands):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False

    # TODO(levysotsky, gritukan): Drop me!
    @authors("levysotsky")
    def test_rename_with_both_columns_in_filter(self):
        pass


class TestMapCommandsCompatNewNodes(BaseTestMapCommands):
    UPLOAD_DEBUG_ARTIFACT_CHUNKS = True

    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False
