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

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/library
)

IF (SANITIZER_TYPE)
    TAG(ya:not_autocheck)
ENDIF()

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/library/peerdir_policy.inc)

END()
