PY3TEST()

PEERDIR(
    contrib/deprecated/python/mypy
    library/python/testing/yatest_common
)

NO_LINT()

TEST_SRCS(test_arcadia.py)

DEPENDS(
    contrib/deprecated/python/mypy/bin/mypy
)

END()
