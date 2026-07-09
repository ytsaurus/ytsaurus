from yt.yt.flow.tests.flow_execute.lib.flow_execute_test_base import FlowExecuteTestBase


# The shared test body lives in the lib; per-package variants share it and only differ in the YT
# package they bind and the feature toggles they set (see the variant subdirs' ya.make files).
class TestFlowExecute(FlowExecuteTestBase):
    pass
