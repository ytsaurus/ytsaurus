"""End-to-end companion-resource test for the Java companion.

The resources are registered via ``PipelineContext.registerResourceClass`` and
reached from the process function through ``RuntimeContext.getResource``; the
scenario itself lives in ``CompanionResourceTestBase``. The companion JVM is
identified by its main class rather than a binary path.
"""

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_java_base import (
    FlowTestJavaBase,
)
from yt.yt.flow.tests.companion.resource.common.companion_resource_test_base import (
    CompanionResourceTestBase,
)


class TestCompanionResource(CompanionResourceTestBase, FlowTestJavaBase):
    JAVA_RUNNER_BINARY_DIR = yatest.common.binary_path(f"{yatest.common.context.project_path}/companion/")
    JAVA_MAIN_CLASS = "tech.ytsaurus.flow.tests.resource.PipelineMain"
    PIPELINE_CONFIG_PATH = yatest.common.source_path(
        f"{yatest.common.context.project_path}/companion/src/main/resources/pipeline.yson"
    )
    COMPANION_CMDLINE_MARKER = "tech.ytsaurus.flow.tests.resource.PipelineMain"
