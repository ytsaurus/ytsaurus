PY3TEST()

INCLUDE(../YaMakeBoilerplateForTestsWithConftest.txt)

TEST_SRCS(
    test_prepare_scheduling_usage.py
)

DEPENDS(
    yt/yt/tools/prepare_scheduling_usage
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

END()
