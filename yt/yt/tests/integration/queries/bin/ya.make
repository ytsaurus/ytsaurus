PY3TEST()

INCLUDE(../../YaMakeBoilerplateForTests.txt)

COPY_FILE(
    yt/yt/tests/conftest_lib/conftest_queries.py conftest.py
)

TEST_SRCS(
    conftest.py
)

PEERDIR(
    yt/yt/tests/integration/queries
)

END()
