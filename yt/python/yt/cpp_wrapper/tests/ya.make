PY23_TEST()

OWNER(g:yt g:yt-python)

SIZE(MEDIUM)

TAG(
    ya:huge_logs
    ya:full_logs
    ya:sys_info
)

REQUIREMENTS(
    ram_disk:4
    ram:16
    cpu:4
)

IF (YT_TEAMCITY)
    TAG(ya:yt)
    YT_SPEC(yt/yt/tests/integration/spec.yson)
ENDIF()

TEST_SRCS(
    test_cpp_operations.py
    conftest.py
)

SRCS(
    jobs.cpp
    data.proto
)

PEERDIR(
    yt/python/yt/cpp_wrapper
    yt/python/client
    yt/python/yt/testlib
)

DEPENDS(
    yt/yt/packages/tests_package
)

END()
