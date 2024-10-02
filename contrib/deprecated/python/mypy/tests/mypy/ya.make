PY3TEST()

PEERDIR(
    contrib/deprecated/python/mypy
    library/python/testing/yatest_common
)

NO_LINT()

TEST_SRCS(
    test_mypy.py
)

DATA (
    sbr://2114583561
)

END()
