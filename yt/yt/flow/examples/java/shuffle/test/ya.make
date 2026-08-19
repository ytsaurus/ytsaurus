PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_java_shuffle.py
    yt_sync.py
)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/../shuffle
    yt/yt/flow/bin/flow_server
)

DATA(arcadia/${MODDIR}/../shuffle/src/main/resources/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
