PY3TEST()

PY_SRCS(
    common.py
)

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
    yql/essentials/udfs/common/string
    yql/essentials/udfs/common/yson2
)

PEERDIR(
    yt/yt/tests/conftest_lib
    yt/python/yt/environment/components/yql_agent
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

IF (SANITIZER_TYPE)
    DEPENDS(
        contrib/libs/llvm18/tools/llvm-symbolizer
    )

    REQUIREMENTS(
        ram:60
    )
ELSE()
    REQUIREMENTS(
        ram:32
    )
ENDIF()

FORK_SUBTESTS()
SPLIT_FACTOR(32)

END()
