PY3TEST()

TEST_SRCS(
    test_multiplexer.py
    yt_sync.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/pipeline
)

DATA(arcadia/${MODDIR}/pipeline/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()

RECURSE(
    pipeline
    test_query
)
