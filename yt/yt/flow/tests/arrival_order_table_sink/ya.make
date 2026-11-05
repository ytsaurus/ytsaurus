PY3TEST()

TEST_SRCS(
    test_pipeline.py
)

SET(YT_CLUSTER_NAMES primary,remote_0)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
)

DEPENDS(
    ${MODDIR}/pipeline
)

DATA(arcadia/${MODDIR}/pipeline/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

SIZE(MEDIUM)

END()

RECURSE(
    pipeline
)
