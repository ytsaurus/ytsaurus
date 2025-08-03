PY3TEST()

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)
    
YT_SPEC(yt/yt/tests/integration/spec_with_network.yson)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

TEST_SRCS(
    test_user_job_network.py
)

END()

