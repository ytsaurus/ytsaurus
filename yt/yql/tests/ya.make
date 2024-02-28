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
)

# In open source these artifacts must be taken from YDB repo or extracted from Query Tracker Docker image.
IF (NOT OPENSOURCE)
    # This sandbox resource is produced by the script yt/yql/package/build_ydb_artifacts_for_tests.sh.
    DATA(sbr://5896064626)
ENDIF()

PEERDIR(
    yt/yt/tests/conftest_lib
)

END()
