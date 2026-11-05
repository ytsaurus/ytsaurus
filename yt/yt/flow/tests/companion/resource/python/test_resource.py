"""End-to-end companion-resource test for the Python companion.

The resources are registered via ``Pipeline.add_resource`` and reached from the
process function through ``ctx.get_resource``; the scenario itself lives in
``CompanionResourceTestBase``.
"""

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import (
    FlowTestPythonBase,
)
from yt.yt.flow.tests.companion.resource.common.companion_resource_test_base import (
    CompanionResourceTestBase,
)

COMPANION_BINARY = yatest.common.binary_path("yt/yt/flow/tests/companion/resource/python/pipeline/pipeline")


class TestCompanionResource(CompanionResourceTestBase, FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = COMPANION_BINARY
    PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")
    COMPANION_CMDLINE_MARKER = COMPANION_BINARY
    # The Python companion pre-forks its serving processes behind one port.
    EXPECTED_COMPANION_PROCESSES = None
