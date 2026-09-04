PY3TEST()

NO_BUILD_IF(SANITIZER_TYPE)

PY_SRCS(
    common.py
)

TEST_SRCS(
    conftest.py
    test_simple.py
    test_udfs.py
    test_ytflow.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/YaMakeBoilerplateForTests.txt)

DEPENDS(
    yt/yt/packages/tests_package
    yt/yql/agent/bin
    yt/yql/tests/agent/throwing_udf
    yt/yql/tools/mrjob
    yt/yql/tools/ytflow_worker

    yql/essentials/udfs/common/datetime2
    yql/essentials/udfs/common/re2
    yql/essentials/udfs/common/file
    yql/essentials/udfs/common/python/python3_small
    yql/essentials/udfs/common/streaming
    yql/essentials/udfs/common/string
    yql/essentials/udfs/common/yson2
    yql/essentials/udfs/test/simple
)

PEERDIR(
    library/python/port_manager
    yql/library/langver/python
    yql/essentials/providers/common/proto
    yt/yt/tests/conftest_lib
    yt/python/yt/environment/components/yql_agent
    yt/yql/tests/common/test_framework
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
SPLIT_FACTOR(64)

ENV(YT_LOCAL=1)

END()
