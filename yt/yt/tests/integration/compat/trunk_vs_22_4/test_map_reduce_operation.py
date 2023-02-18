from original_tests.yt.yt.tests.integration.controller.test_map_reduce_operation \
    import TestSchedulerMapReduceCommands as BaseTestMapReduceCommands
from yt.common import update
from yt_commands import authors


class TestMapReduceCommandsCompatNewCA(BaseTestMapReduceCommands):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }

    DELTA_CONTROLLER_AGENT_CONFIG = update(BaseTestMapReduceCommands.DELTA_CONTROLLER_AGENT_CONFIG, {
        "controller_agent": {
            "operation_options": {
                "spec_template": {
                    "enable_table_index_if_has_trivial_mapper": True,
                },
            },
        },
    })

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False

    @authors("levysotsky")
    def test_several_intermediate_schemas_failing(self):
        pass


class TestMapReduceCommandsCompatNewNodes(BaseTestMapReduceCommands):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }

    DELTA_CONTROLLER_AGENT_CONFIG = update(BaseTestMapReduceCommands.DELTA_CONTROLLER_AGENT_CONFIG, {
        "controller_agent": {
            "operation_options": {
                "spec_template": {
                    "enable_table_index_if_has_trivial_mapper": True,
                },
            },
        },
    })

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False

    # TODO(gritukan, levysotsky): Drop me!
    @authors("levysotsky")
    def test_several_intermediate_schemas_trivial_mapper(self):
        pass
