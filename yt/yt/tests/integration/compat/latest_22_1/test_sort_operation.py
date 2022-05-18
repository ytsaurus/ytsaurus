from original_tests.yt.yt.tests.integration.tests.controller.test_sort_operation \
    import TestSchedulerSortCommands as BaseTestSortCommands
from yt.common import update


class TestSortCommandsCompatUpToCA(BaseTestSortCommands):
    ARTIFACT_COMPONENTS = {
        "22_1": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }

    DELTA_CONTROLLER_AGENT_CONFIG = update(BaseTestSortCommands.DELTA_CONTROLLER_AGENT_CONFIG, {
        "controller_agent": {
            "enable_table_column_renaming": False,
        },
    })
