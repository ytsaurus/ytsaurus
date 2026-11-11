PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/pipeline
    yt/yt/flow/tools/draw_pipeline_graph
    yt/yt/flow/tools/job_investigation
)

DATA(
    arcadia/${MODDIR}/pipeline/pipeline.yson
)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()

RECURSE(
    pipeline
)
