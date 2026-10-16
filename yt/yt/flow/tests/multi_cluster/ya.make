PY3TEST()

TEST_SRCS(
    test_pipeline.py
    yt_sync.py
)

SET(YT_CLUSTER_NAMES primary,remote_0,remote_1)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/pipeline
    yt/python/yt/wrapper/bin/yt_make
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
)
