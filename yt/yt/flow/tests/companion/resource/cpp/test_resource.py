"""End-to-end companion-resource test for the C++ companion.

The resources are registered via ``TPipeline::AddResource`` and reached from the
process function through ``IRuntimeInitContext::GetStaticResource``; the
scenario itself lives in ``CompanionResourceTestBase``.
"""

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_cpp_base import (
    FlowTestCppCompanionBase,
)
from yt.yt.flow.tests.companion.resource.common.companion_resource_test_base import (
    CompanionResourceTestBase,
)

COMPANION_BINARY = yatest.common.binary_path("yt/yt/flow/tests/companion/resource/cpp/companion/companion")


class TestCompanionResource(CompanionResourceTestBase, FlowTestCppCompanionBase):
    CPP_COMPANION_BINARY = COMPANION_BINARY
    PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")
    COMPANION_CMDLINE_MARKER = COMPANION_BINARY
