PY3TEST()

SET(YT_CLUSTER_NAMES primary,remote_0)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_java_wait_click_join.py
    yt_sync.py
)

PEERDIR(
    yt/yt/flow/library/python/queue
)

DEPENDS(
    ${MODDIR}/../wait_click_join
    yt/yt/flow/bin/flow_server
)

DATA(arcadia/${MODDIR}/../wait_click_join/src/main/resources/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
