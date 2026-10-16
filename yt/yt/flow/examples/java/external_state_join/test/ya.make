PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_java_external_state_join.py
    yt_sync.py
)

DEPENDS(
    ${MODDIR}/../external_state_join
    yt/yt/flow/bin/flow_server
)

DATA(arcadia/${MODDIR}/../external_state_join/src/main/resources/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
