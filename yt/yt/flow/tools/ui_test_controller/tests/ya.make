PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tools/ui_test_controller/fixture_resource.inc)

SIZE(SMALL)

TEST_SRCS(
    test_fixtures.py
)

PEERDIR(
    yt/python/yt/wrapper
)

DATA(
    sbr://${FLOW_UI_FIXTURE_RESOURCE_ID}
    arcadia/yt/yt/flow/tools/ui_test_controller/fixture_pipeline/lib/computation.cpp
    arcadia/yt/yt/flow/tools/ui_test_controller/fixture_pipeline/lib/computation.h
    arcadia/yt/yt/flow/tools/ui_test_controller/fixture_pipeline/pipeline/pipeline.yson
    arcadia/yt/yt/flow/tools/ui_test_controller/fixture_pipeline/test_capture.py
)

END()
