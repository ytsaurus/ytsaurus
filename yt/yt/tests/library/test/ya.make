PY3TEST()


COPY_FILE(
    yt/yt/tests/conftest_lib/conftest.py conftest.py
)

TEST_SRCS(
    test_library.py
    conftest.py
)

DEPENDS(
    yt/yt/packages/tests_package
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/tests/library
)

END()
