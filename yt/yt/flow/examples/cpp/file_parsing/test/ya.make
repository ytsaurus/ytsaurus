PY3TEST()

TEST_SRCS(
    test_file_parsing.py
)

SET(YT_CLUSTER_NAMES primary)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/..
)

DATA(
    arcadia/${MODDIR}/../pipeline.yson
)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
