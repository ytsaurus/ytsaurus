PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_java_noop.py
    yt_sync.py
)

DEPENDS(
    ${MODDIR}/../noop
    yt/yt/flow/bin/flow_server
)

DATA(arcadia/${MODDIR}/../pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()
