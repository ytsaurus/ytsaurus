PY3TEST()

TEST_SRCS(
    test_migration.py
)

PEERDIR(
    contrib/python/allure-pytest
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/YaMakeBoilerplateForTests.txt)

END()
