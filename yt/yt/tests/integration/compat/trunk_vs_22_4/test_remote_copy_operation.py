from original_tests.yt.yt.tests.integration.controller.test_remote_copy_operation \
    import TestSchedulerRemoteCopyCommands as BaseTestRemoteCopyCommands

from yt_commands import authors


class TestRemoteCopyCommandsCompatUpNewCA(BaseTestRemoteCopyCommands):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False

    @authors("akozhikhov")
    def test_seed_replicas(self):
        pass


class TestRemoteCopyCommandsCompatNewNodes(BaseTestRemoteCopyCommands):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False

    @authors("eshcherbin")
    def test_user_slots_validation(self):
        pass
