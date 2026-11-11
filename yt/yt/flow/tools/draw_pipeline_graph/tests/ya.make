PY3TEST()

STYLE_PYTHON()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tools/ui_test_controller/fixture_resource.inc)

SIZE(SMALL)

TEST_SRCS(
    test_bindings.py
)

PEERDIR(
    yt/python/yt/wrapper
    yt/yt/flow/tools/draw_pipeline_graph/bindings
)

DATA(
    sbr://${FLOW_UI_FIXTURE_RESOURCE_ID}
)

END()
