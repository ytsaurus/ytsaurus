PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_http.py
    yt_sync.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    yt/yt/flow/bin/flow_server
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
