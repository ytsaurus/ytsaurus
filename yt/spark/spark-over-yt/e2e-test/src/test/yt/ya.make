PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/YaMakeBoilerplateForTests.txt)

PEERDIR(
    yt/yt/tests/conftest_lib
    yt/yt/tests/library
    yt/spark/spark-over-yt/data-source/src/main
    yt/spark/spark-over-yt/tools/release
)

TEST_SRCS(
    base.py
    test_base_jobs.py
    test_cluster.py
)

DEPENDS(
    yt/spark/spark-over-yt/e2e-test/src/test/yt/data
)

END()
