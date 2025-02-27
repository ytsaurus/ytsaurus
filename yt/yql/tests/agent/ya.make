PY3TEST()

TEST_SRCS(
    conftest.py
    test_simple.py
    test_udfs.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/YaMakeBoilerplateForTests.txt)

DEPENDS(
    yt/yt/packages/tests_package
    yt/yql/agent/bin

    yt/yql/plugin/dynamic
    yt/yql/tools/mrjob
    yql/essentials/udfs/common/re2
    yql/essentials/udfs/common/file
    yql/essentials/udfs/common/python/python3_small
)

PEERDIR(
    yt/yt/tests/conftest_lib
    yt/python/yt/environment/components/yql_agent
)

IF (SANITIZER_TYPE == "address" OR SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck)
ENDIF()

FORK_SUBTESTS()
SPLIT_FACTOR(4)

END()
